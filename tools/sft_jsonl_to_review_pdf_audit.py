#!/usr/bin/env python3
"""
Convert SFT JSONL output to a Persian RTL audit/review PDF.

The converter joins every generated pair to its source chunk by
`source_pair.source_chunk_id`, so the displayed book/page reference comes from
the chunker output instead of being guessed from the generated answer.

Required dependency:
    pip install PyMuPDF
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import fitz


PERSIAN_DIGITS = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")
REFERENCE_MARKERS = (
    "Reference answer:\n",
    "Reference Answer:\n",
    "متن مرجع:\n",
)


@dataclass
class ReviewItem:
    number: int
    pair_id: str
    source_chunk_id: str
    question: str
    answer: str
    reference_text: str
    source_name: str
    pages: str
    section: str
    task_category: str
    decision: str
    quality_decision: str
    selected_for_dataset: str
    score: str


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def fa_digits(value: Any) -> str:
    return str(value).translate(PERSIAN_DIGITS)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def remove_unsupported_emoji(value: str) -> str:
    # DejaVu / Noto Arabic fonts used on most Linux servers do not contain
    # pictographic emoji. Removing only emoji planes avoids empty square glyphs
    # while retaining ordinary mathematical and typographic symbols.
    return re.sub(
        "["
        "\U0001F000-\U0001FAFF"
        "\U0001FC00-\U0001FFFF"
        "\U00002600-\U000026FF"
        "\U00002700-\U000027BF"
        "]+",
        "",
        value,
    )


def read_jsonl(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    with path.open("r", encoding="utf-8-sig") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append(f"line {line_number}: {exc}")
                continue
            if not isinstance(value, dict):
                errors.append(f"line {line_number}: JSON value is not an object")
                continue
            rows.append(value)
    return rows, errors


def index_chunks(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        for key in ("id", "original_id", "chunk_id"):
            value = clean_text(row.get(key))
            if value:
                index[value] = row
    return index


def first_text(*values: Any) -> str:
    for value in values:
        text = clean_text(value)
        if text:
            return text
    return ""


def choose_language_block(row: dict[str, Any], language: str) -> dict[str, Any]:
    if language == "fa":
        candidate = row.get("persian")
        return candidate if isinstance(candidate, dict) else {}
    if language == "en":
        candidate = row.get("english")
        return candidate if isinstance(candidate, dict) else {}

    # Auto: Persian is preferred for this review format. If it is absent,
    # retain compatibility with pre-translation and generic pair schemas.
    for key in ("persian", "english"):
        candidate = row.get(key)
        if isinstance(candidate, dict) and (
            clean_text(candidate.get("question"))
            or clean_text(candidate.get("instruction"))
        ):
            return candidate
    return {}


def source_chunk_id(row: dict[str, Any]) -> str:
    source_pair = row.get("source_pair")
    metadata = row.get("metadata")
    source_pair = source_pair if isinstance(source_pair, dict) else {}
    metadata = metadata if isinstance(metadata, dict) else {}
    return first_text(
        source_pair.get("source_chunk_id"),
        metadata.get("source_sample_id"),
        row.get("source_chunk_id"),
        row.get("chunk_id"),
    )


def extract_embedded_reference(row: dict[str, Any]) -> str:
    source_pair = row.get("source_pair")
    source_pair = source_pair if isinstance(source_pair, dict) else {}
    source_text = first_text(
        source_pair.get("source_text"),
        row.get("source_text"),
        row.get("reference"),
        row.get("reference_text"),
    )
    if not source_text:
        return ""
    for marker in REFERENCE_MARKERS:
        if marker in source_text:
            return clean_text(source_text.split(marker, 1)[1])
    return source_text


def compress_numbers(values: Iterable[Any], offset: int = 0) -> str:
    numbers: list[int] = []
    seen: set[int] = set()
    for value in values:
        try:
            number = int(value) + offset
        except (TypeError, ValueError):
            continue
        if number not in seen:
            seen.add(number)
            numbers.append(number)
    if not numbers:
        return ""
    numbers.sort()
    ranges: list[str] = []
    start = previous = numbers[0]
    for number in numbers[1:]:
        if number == previous + 1:
            previous = number
            continue
        ranges.append(
            fa_digits(start)
            if start == previous
            else f"{fa_digits(start)} تا {fa_digits(previous)}"
        )
        start = previous = number
    ranges.append(
        fa_digits(start)
        if start == previous
        else f"{fa_digits(start)} تا {fa_digits(previous)}"
    )
    return "، ".join(ranges)


def chunk_pages(chunk: dict[str, Any], offset: int) -> str:
    labels = chunk.get("page_labels")
    if isinstance(labels, list) and labels:
        pages = compress_numbers(labels, offset)
        if pages:
            return pages
    ps = chunk.get("ps")
    pe = chunk.get("pe")
    if ps is None and pe is None:
        return ""
    if pe is None:
        pe = ps
    return compress_numbers([ps, pe], offset)


def chunk_section(chunk: dict[str, Any]) -> str:
    section = chunk.get("sec")
    if isinstance(section, list):
        return " / ".join(clean_text(part) for part in section if clean_text(part))
    return clean_text(section)


def build_items(
    pair_rows: list[dict[str, Any]],
    chunks: dict[str, dict[str, Any]],
    *,
    language: str,
    include_rejected: bool,
    page_offset: int,
    limit: int,
) -> tuple[list[ReviewItem], list[str]]:
    items: list[ReviewItem] = []
    warnings: list[str] = []
    missing_chunks: set[str] = set()

    for row in pair_rows:
        decision = first_text(row.get("final_decision"), row.get("decision"))
        if not include_rejected and decision.lower() in {"reject", "rejected", "drop"}:
            continue

        block = choose_language_block(row, language)
        source_pair = row.get("source_pair")
        source_pair = source_pair if isinstance(source_pair, dict) else {}

        question = first_text(
            block.get("question"),
            block.get("instruction"),
            row.get("question"),
            row.get("instruction"),
            source_pair.get("instruction"),
        )
        answer = first_text(
            block.get("response"),
            block.get("answer"),
            row.get("response"),
            row.get("answer"),
            source_pair.get("response"),
        )
        if not question or not answer:
            warnings.append(
                f"pair {first_text(row.get('id'), '?')}: missing question or answer"
            )
            continue

        chunk_id = source_chunk_id(row)
        chunk = chunks.get(chunk_id, {})
        if chunk_id and not chunk:
            missing_chunks.add(chunk_id)

        reference_text = first_text(chunk.get("text"), extract_embedded_reference(row))
        source_name = first_text(
            chunk.get("src"),
            chunk.get("source_file"),
            row.get("source_file"),
            "نامشخص",
        )
        pages = chunk_pages(chunk, page_offset)
        if not pages:
            # Compatibility with metadata embedded by older runners.
            metadata = row.get("metadata")
            metadata = metadata if isinstance(metadata, dict) else {}
            pages = first_text(metadata.get("pages"), source_pair.get("pages"), "نامشخص")

        task_category = first_text(
            row.get("task_category"),
            source_pair.get("task_type"),
            source_pair.get("task_category"),
        )

        raw_score = row.get("final_score")
        score = ""
        if isinstance(raw_score, (float, int)):
            score = fa_digits(f"{raw_score:g}")
        elif raw_score is not None:
            score = clean_text(raw_score)

        quality_decision = first_text(row.get("quality_decision"), decision)
        selected_raw = row.get("selected_for_dataset")
        if selected_raw is True:
            selected_text = "بله"
        elif selected_raw is False:
            selected_text = "خیر"
        else:
            selected_text = ""

        items.append(
            ReviewItem(
                number=len(items) + 1,
                pair_id=first_text(row.get("id"), source_pair.get("pair_id")),
                source_chunk_id=chunk_id,
                question=question,
                answer=answer,
                reference_text=reference_text,
                source_name=source_name,
                pages=pages,
                section=chunk_section(chunk),
                task_category=task_category,
                decision=decision,
                quality_decision=quality_decision,
                selected_for_dataset=selected_text,
                score=score,
            )
        )
        if limit > 0 and len(items) >= limit:
            break

    if missing_chunks:
        examples = ", ".join(sorted(missing_chunks)[:5])
        warnings.append(
            f"{len(missing_chunks)} source_chunk_id values were not found in chunks JSONL"
            f" (examples: {examples})"
        )
    return items, warnings


def html_text(value: str) -> str:
    safe = html.escape(remove_unsupported_emoji(value), quote=True)
    # Keep a deliberately small Markdown subset. The generated data frequently
    # uses **bold** and "* item" lists, and rendering the raw markers makes
    # human review harder.
    safe = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", safe)
    lines: list[str] = []
    for line in safe.split("\n"):
        if re.match(r"^\s*[*-]\s+", line):
            line = re.sub(r"^\s*[*-]\s+", "• ", line)
        lines.append(line)
    return "<br>".join(lines)


def make_document_html(
    items: list[ReviewItem],
    *,
    book_title: str,
    include_reference_text: bool,
    reference_max_chars: int,
    show_ids: bool,
) -> str:
    blocks: list[str] = []

    for item in items:
        display_source = book_title or item.source_name
        meta_parts = [
            f"<b>کتاب:</b> {html_text(display_source)}",
            f"<b>صفحات:</b> {html_text(item.pages)}",
        ]
        if item.section:
            meta_parts.append(f"<b>موضوع:</b> {html_text(item.section)}")


        if item.task_category:
            meta_parts.append(
                f"<b>نوع تسک:</b> {html_text(item.task_category)}"
            )

        decision_map = {
            "accept": "پذیرفته",
            "accepted": "پذیرفته",
            "keep": "پذیرفته",
            "reject": "رد شده",
            "rejected": "رد شده",
            "drop": "رد شده",
        }
        decision_key = clean_text(item.decision).lower()
        decision_fa = decision_map.get(decision_key, clean_text(item.decision) or "نامشخص")
        meta_parts.append(f"<b>تصمیم نهایی:</b> {html_text(decision_fa)}")

        quality_key = clean_text(item.quality_decision).lower()
        quality_fa = decision_map.get(quality_key, clean_text(item.quality_decision) or "نامشخص")
        if quality_fa and quality_fa != decision_fa:
            meta_parts.append(f"<b>تصمیم کیفی:</b> {html_text(quality_fa)}")

        if item.selected_for_dataset:
            meta_parts.append(
                f"<b>انتخاب دیتاست:</b> {html_text(item.selected_for_dataset)}"
            )

        if item.score:
            meta_parts.append(f"<b>امتیاز:</b> {html_text(item.score)}")

        blocks.extend(
            [
                "<article class='qa'>",
                f"<div class='question-label'>سؤال {fa_digits(item.number)}:</div>",
                f"<div class='question'>{html_text(item.question)}</div>",
                f"<div class='meta'>{' &nbsp; | &nbsp; '.join(meta_parts)}</div>",
                f"<div class='answer'>{html_text(item.answer)}</div>",
            ]
        )
        if include_reference_text and item.reference_text:
            reference = item.reference_text
            truncated = False
            if reference_max_chars > 0 and len(reference) > reference_max_chars:
                reference = reference[:reference_max_chars].rstrip()
                truncated = True
            suffix = " …" if truncated else ""
            blocks.extend(
                [
                    "<div class='reference-title'>متن مرجع:</div>",
                    f"<div class='reference'>{html_text(reference)}{suffix}</div>",
                ]
            )
        if show_ids:
            blocks.append(
                "<div class='ids'>"
                f"pair_id: {html_text(item.pair_id)}"
                f" &nbsp; | &nbsp; chunk_id: {html_text(item.source_chunk_id)}"
                "</div>"
            )
        blocks.append("</article>")

    return (
        "<!doctype html><html lang='fa' dir='rtl'><head>"
        "<meta charset='utf-8'></head><body>"
        + "".join(blocks)
        + "</body></html>"
    )


def make_cover_html(book_title: str, item_count: int) -> str:
    generated = fa_digits(datetime.now().strftime("%Y/%m/%d - %H:%M"))
    return (
        "<!doctype html><html lang='fa' dir='rtl'><head>"
        "<meta charset='utf-8'></head><body>"
        "<section class='cover'>"
        "<h1>استخراج پرسش و پاسخ با هوش مصنوعی</h1>"
        f"<h2>کتاب: {html_text(book_title)}</h2>"
        f"<p class='cover-meta'>تعداد نمونه‌ها: {fa_digits(item_count)}</p>"
        f"<p class='cover-meta'>{generated}</p>"
        "</section></body></html>"
    )


def resolve_fonts(
    font_regular: Path | None, font_bold: Path | None
) -> tuple[Path, Path]:
    regular_candidates = [
        font_regular,
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/truetype/noto/NotoNaskhArabic-Regular.ttf"),
        Path("/usr/share/fonts/opentype/noto/NotoNaskhArabic-Regular.ttf"),
    ]
    bold_candidates = [
        font_bold,
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
        Path("/usr/share/fonts/truetype/noto/NotoNaskhArabic-Bold.ttf"),
        Path("/usr/share/fonts/opentype/noto/NotoNaskhArabic-Bold.ttf"),
    ]
    regular = next((p for p in regular_candidates if p and p.is_file()), None)
    bold = next((p for p in bold_candidates if p and p.is_file()), None)
    if not regular:
        raise FileNotFoundError(
            "No Persian-capable regular font found. Pass --font-regular /path/font.ttf"
        )
    if not bold:
        bold = regular
    return regular.resolve(), bold.resolve()


def copy_fonts_to_temp(
    regular: Path, bold: Path, temp_dir: Path
) -> tuple[str, str, fitz.Archive]:
    regular_name = "review-regular" + regular.suffix.lower()
    bold_name = "review-bold" + bold.suffix.lower()
    (temp_dir / regular_name).write_bytes(regular.read_bytes())
    if bold.resolve() == regular.resolve():
        bold_name = regular_name
    else:
        (temp_dir / bold_name).write_bytes(bold.read_bytes())
    return regular_name, bold_name, fitz.Archive(str(temp_dir))


def write_pdf(
    document_html: str,
    cover_html: str,
    output: Path,
    *,
    regular_font: Path,
    bold_font: Path,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    a4 = fitz.paper_rect("a4")
    content_rect = fitz.Rect(38, 42, a4.width - 38, a4.height - 42)

    with tempfile.TemporaryDirectory(prefix="sft-review-pdf-") as tmp_name:
        tmp_dir = Path(tmp_name)
        regular_name, bold_name, archive = copy_fonts_to_temp(
            regular_font, bold_font, tmp_dir
        )
        css = f"""
        @font-face {{
          font-family: ReviewFa;
          src: url('{regular_name}');
          font-weight: normal;
        }}
        @font-face {{
          font-family: ReviewFa;
          src: url('{bold_name}');
          font-weight: bold;
        }}
        * {{
          box-sizing: border-box;
        }}
        body {{
          font-family: ReviewFa;
          direction: rtl;
          /* PyMuPDF Story currently mirrors left/right alignment for RTL text.
             Using left here renders Persian blocks visually right-aligned. */
          text-align: left;
          font-size: 10.2pt;
          line-height: 1.65;
          color: #111827;
          margin: 0;
          padding: 0;
        }}
        .cover {{
          text-align: center;
          padding-top: 105pt;
        }}
        .cover h1 {{
          font-size: 23pt;
          line-height: 1.45;
          margin: 0 0 15pt 0;
        }}
        .cover h2 {{
          font-size: 15pt;
          line-height: 1.55;
          margin: 0 0 18pt 0;
        }}
        .cover-meta {{
          color: #6b7280;
          font-size: 9pt;
          margin: 3pt 0;
        }}
        .qa {{
          margin: 0 0 20pt 0;
          padding-top: 2pt;
        }}
        .question-label {{
          direction: rtl;
          text-align: left;
          font-weight: bold;
          font-size: 12pt;
          margin: 0 0 4pt 0;
        }}
        .question {{
          direction: rtl;
          text-align: left;
          font-size: 10.6pt;
          line-height: 1.7;
          margin: 0 0 4pt 0;
        }}
        .meta {{
          direction: rtl;
          text-align: left;
          color: #6b7280;
          font-size: 8.3pt;
          line-height: 1.55;
          margin: 0 0 6pt 0;
        }}
        .answer {{
          direction: rtl;
          text-align: left;
          border: 0.8pt solid #111827;
          background: #f8fafc;
          padding: 8pt 10pt;
          line-height: 1.75;
          margin: 0 0 6pt 0;
        }}
        .reference-title {{
          direction: rtl;
          text-align: left;
          color: #334155;
          font-size: 9pt;
          font-weight: bold;
          margin: 5pt 0 2pt 0;
        }}
        .reference {{
          direction: rtl;
          text-align: left;
          border-right: 2.2pt solid #94a3b8;
          background: #f1f5f9;
          color: #334155;
          font-size: 8.7pt;
          line-height: 1.65;
          padding: 6pt 9pt;
          margin: 0 0 4pt 0;
        }}
        .ids {{
          direction: ltr;
          text-align: left;
          color: #94a3b8;
          font-size: 6.8pt;
          margin-top: 3pt;
        }}
        """
        story = fitz.Story(document_html, user_css=css, em=10, archive=archive)
        raw_pdf = tmp_dir / "raw.pdf"
        writer = fitz.DocumentWriter(str(raw_pdf))

        def rect_function(
            rect_number: int, filled: fitz.Rect
        ) -> tuple[fitz.Rect, fitz.Rect, fitz.Matrix | None]:
            del rect_number, filled
            return a4, content_rect, None

        story.write(writer, rect_function)
        writer.close()

        # Assemble a dedicated cover page, then append Story's flowing content.
        content_document = fitz.open(raw_pdf)
        document = fitz.open()
        cover_page = document.new_page(width=a4.width, height=a4.height)
        spare_height, scale = cover_page.insert_htmlbox(
            content_rect,
            cover_html,
            css=css,
            archive=archive,
            scale_low=0,
        )
        if spare_height < 0 or scale < 0.99:
            raise RuntimeError("cover page did not fit at full scale")
        document.insert_pdf(content_document)
        content_document.close()

        # Add compact, non-RTL page numbers after layout is complete.
        total = document.page_count
        for index, page in enumerate(document):
            footer = f"{index + 1} / {total}"
            page.insert_textbox(
                fitz.Rect(0, a4.height - 25, a4.width, a4.height - 9),
                footer,
                fontsize=7,
                fontname="helv",
                color=(0.45, 0.48, 0.52),
                align=fitz.TEXT_ALIGN_CENTER,
            )
        final_temp = tmp_dir / "final.pdf"
        document.save(final_temp, garbage=4, deflate=True)
        document.close()
        os.replace(final_temp, output)


def validate_book_pdf(path: Path | None, items: list[ReviewItem]) -> list[str]:
    if path is None:
        return []
    warnings: list[str] = []
    if not path.is_file():
        return [f"book PDF not found: {path}"]
    try:
        document = fitz.open(path)
        page_count = document.page_count
        document.close()
    except Exception as exc:
        return [f"could not open book PDF: {exc}"]
    if page_count <= 0:
        warnings.append("book PDF has no pages")
    eprint(f"[BOOK_PDF] pages={page_count} path={path}")
    eprint(
        "[BOOK_PDF] Note: the displayed references come from chunks.jsonl "
        "(page_labels/ps/pe); the PDF is used only for validation."
    )
    return warnings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a Persian QA review PDF from pipeline SFT JSONL and "
            "chunker chunks.jsonl."
        )
    )
    parser.add_argument("--pairs", required=True, type=Path, help="SFT pairs JSONL")
    parser.add_argument(
        "--chunks", required=True, type=Path, help="chunker chunks.jsonl"
    )
    parser.add_argument("--output", required=True, type=Path, help="output PDF")
    parser.add_argument(
        "--book-pdf",
        type=Path,
        default=None,
        help="optional original book PDF for page-count validation",
    )
    parser.add_argument(
        "--book-title",
        default="",
        help="title shown on the cover; defaults to the source name",
    )
    parser.add_argument(
        "--language",
        choices=("auto", "fa", "en"),
        default="auto",
        help="pair language block to render (default: auto, Persian first)",
    )
    parser.add_argument(
        "--page-offset",
        type=int,
        default=0,
        help="integer added to every page label before display (default: 0)",
    )
    parser.add_argument(
        "--include-reference-text",
        action="store_true",
        help="also print the source chunk text below each answer",
    )
    parser.add_argument(
        "--reference-max-chars",
        type=int,
        default=0,
        help="truncate printed reference text; 0 means no truncation",
    )
    parser.add_argument(
        "--include-rejected",
        action="store_true",
        help="include rows whose final_decision is reject",
    )
    parser.add_argument(
        "--show-ids",
        action="store_true",
        help="print pair_id and source_chunk_id below each item",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="render only the first N valid items; 0 means all",
    )
    parser.add_argument("--font-regular", type=Path, default=None)
    parser.add_argument("--font-bold", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    for required in (args.pairs, args.chunks):
        if not required.is_file():
            eprint(f"ERROR: input file not found: {required}")
            return 2

    pair_rows, pair_errors = read_jsonl(args.pairs)
    chunk_rows, chunk_errors = read_jsonl(args.chunks)
    chunks = index_chunks(chunk_rows)

    items, item_warnings = build_items(
        pair_rows,
        chunks,
        language=args.language,
        include_rejected=args.include_rejected,
        page_offset=args.page_offset,
        limit=args.limit,
    )
    warnings = pair_errors + chunk_errors + item_warnings
    warnings.extend(validate_book_pdf(args.book_pdf, items))

    if not items:
        eprint("ERROR: no renderable question/answer pairs were found")
        for warning in warnings:
            eprint("WARNING:", warning)
        return 3

    book_title = clean_text(args.book_title)
    if not book_title:
        book_title = items[0].source_name

    regular_font, bold_font = resolve_fonts(args.font_regular, args.font_bold)
    document_html = make_document_html(
        items,
        book_title=book_title,
        include_reference_text=args.include_reference_text,
        reference_max_chars=max(0, args.reference_max_chars),
        show_ids=args.show_ids,
    )
    cover_html = make_cover_html(book_title, len(items))
    write_pdf(
        document_html,
        cover_html,
        args.output.resolve(),
        regular_font=regular_font,
        bold_font=bold_font,
    )

    print(
        f"[DONE] pairs={len(items)} chunks={len(chunks)} "
        f"warnings={len(warnings)} output={args.output.resolve()}"
    )
    for warning in warnings[:20]:
        eprint("WARNING:", warning)
    if len(warnings) > 20:
        eprint(f"WARNING: {len(warnings) - 20} additional warnings omitted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
