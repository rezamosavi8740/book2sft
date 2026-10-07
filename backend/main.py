from __future__ import annotations

import asyncio
import copy
import json
import os
import shutil
import sys
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data/jobs"))
CHUNKER_BASE_URL = os.getenv("CHUNKER_BASE_URL", "http://host.docker.internal:8080").rstrip("/")
SFT_ORCHESTRATOR_BASE_URL = os.getenv("SFT_ORCHESTRATOR_BASE_URL", "http://host.docker.internal:9100").rstrip("/")
SFT_GENERATOR_BASE_URL = os.getenv("SFT_GENERATOR_BASE_URL", "http://host.docker.internal:9102").rstrip("/")
CHUNKER_TIMEOUT_SEC = env_float("CHUNKER_TIMEOUT_SEC", 7200.0)
SFT_TIMEOUT_SEC = env_float("SFT_TIMEOUT_SEC", 7200.0)
POLL_INTERVAL_SEC = max(0.25, env_float("POLL_INTERVAL_SEC", 1.0))
SFT_TELEMETRY_POLL_SEC = max(0.4, env_float("SFT_TELEMETRY_POLL_SEC", 0.8))
JOB_WORKERS = max(1, env_int("JOB_WORKERS", 2))
MAX_UPLOAD_MB = max(1, env_int("MAX_UPLOAD_MB", 500))
SFT_CONCURRENCY = max(1, env_int("SFT_CONCURRENCY", 4))
SFT_PAIR_CONCURRENCY = max(1, env_int("SFT_PAIR_CONCURRENCY", 2))
SFT_MAX_PAIRS_PER_CHUNK = max(1, env_int("SFT_MAX_PAIRS_PER_CHUNK", 3))
SFT_BOOK_MAX_PAIRS = max(0, env_int("SFT_BOOK_MAX_PAIRS", 0))
SFT_MAX_PER_CONCEPT = max(1, env_int("SFT_MAX_PER_CONCEPT", 2))
SFT_REQUEST_RETRIES = max(0, env_int("SFT_REQUEST_RETRIES", 0))
SFT_SOURCE_LANGUAGE = os.getenv("SFT_SOURCE_LANGUAGE", "fa")
SFT_TARGET_LANGUAGE = os.getenv("SFT_TARGET_LANGUAGE", "fa")
SFT_DEFAULT_DOMAIN = os.getenv("SFT_DEFAULT_DOMAIN", "general")
PDF_SCRIPT = Path(os.getenv("SFT_REVIEW_PDF_SCRIPT", "/app/tools/sft_jsonl_to_review_pdf_audit.py"))


STAGE_ORDER = [
    "queued",
    "upload",
    "chunker_submit",
    "ocr",
    "normalizing",
    "chunking",
    "book_plan",
    "atomizing",
    "planning",
    "validating",
    "chunker_finalizing",
    "chunker_artifacts",
    "sft_prepare",
    "sft_generation",
    "sft_book_selection",
    "sft_finalize",
    "review_pdf",
    "completed",
]

# Overall progress is a weighted pipeline estimate. Stage-local progress is based
# on real downstream work (Chunker progress, completed SFT chunks, and live
# target telemetry). The weights sum to 1.0 and are exposed to the UI as an
# estimate, not as a claim that every book has identical stage cost.
STAGE_WEIGHTS = {
    "upload": 0.02,
    "chunker_submit": 0.01,
    "ocr": 0.10,
    "normalizing": 0.02,
    "chunking": 0.03,
    "book_plan": 0.03,
    "atomizing": 0.08,
    "planning": 0.06,
    "validating": 0.02,
    "chunker_finalizing": 0.01,
    "chunker_artifacts": 0.02,
    "sft_prepare": 0.02,
    "sft_generation": 0.45,
    "sft_book_selection": 0.04,
    "sft_finalize": 0.04,
    "review_pdf": 0.05,
}

CHUNKER_STAGE_MAP = {
    "ocr": "ocr",
    "normalizing": "normalizing",
    "chunking": "chunking",
    "book_plan": "book_plan",
    "atomizing": "atomizing",
    "planning": "planning",
    "validating": "validating",
    "finalizing": "chunker_finalizing",
}


def empty_timing() -> dict[str, Any]:
    return {
        "started_at": None,
        "finished_at": None,
        "elapsed_sec": 0.0,
        "status": "waiting",
    }


def jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8-sig") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except Exception as exc:
                raise RuntimeError(f"Invalid JSONL at {path}:{line_no}: {exc}") from exc
            if isinstance(value, dict):
                rows.append(value)
    return rows


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp.replace(path)


def first_text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def source_chunk_id(pair: dict[str, Any]) -> str:
    source_pair = pair.get("source_pair") if isinstance(pair.get("source_pair"), dict) else {}
    metadata = pair.get("metadata") if isinstance(pair.get("metadata"), dict) else {}
    return first_text(
        source_pair.get("source_chunk_id"),
        metadata.get("source_sample_id"),
        pair.get("source_chunk_id"),
        pair.get("chunk_id"),
    )


def pair_question_answer(pair: dict[str, Any]) -> tuple[str, str]:
    blocks: list[dict[str, Any]] = []
    for key in ("persian", "english"):
        value = pair.get(key)
        if isinstance(value, dict):
            blocks.append(value)
    source_pair = pair.get("source_pair") if isinstance(pair.get("source_pair"), dict) else {}
    blocks.extend([pair, source_pair])
    for block in blocks:
        q = first_text(block.get("question"), block.get("instruction"), block.get("prompt"))
        a = first_text(block.get("response"), block.get("answer"), block.get("completion"))
        if q and a:
            return q, a
    return "", ""


def nested_numeric_timings(value: Any, prefix: str = "") -> dict[str, float]:
    out: dict[str, float] = {}
    if not isinstance(value, dict):
        return out
    for key, item in value.items():
        full = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, dict):
            out.update(nested_numeric_timings(item, full))
        elif isinstance(item, (int, float)) and not isinstance(item, bool) and (
            str(key).endswith("_sec") or str(key) in {"wall_sec", "elapsed_sec", "total_sec"}
        ):
            out[full] = float(item)
    return out


@dataclass
class JobState:
    job_id: str
    filename: str
    size_bytes: int
    book_title: str = ""
    category: str = ""
    domain: str = "general"
    created_at: str = field(default_factory=utcnow)
    updated_at: str = field(default_factory=utcnow)
    started_at: str | None = None
    completed_at: str | None = None
    status: str = "queued"
    stage: str = "queued"
    progress: float = 0.0
    stage_progress: float = 0.0
    message: str = "Queued"
    eta_sec: float | None = None
    error: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    downstream_job_id: str | None = None
    timings: dict[str, dict[str, Any]] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        now_mono = time.monotonic()
        timings: dict[str, dict[str, Any]] = {}
        for name, raw in self.timings.items():
            clean = {k: v for k, v in raw.items() if not k.startswith("_")}
            if raw.get("status") == "running" and raw.get("_started_mono") is not None:
                clean["elapsed_sec"] = round(now_mono - float(raw["_started_mono"]), 3)
            timings[name] = clean

        total_elapsed_sec: float | None = None
        if self.started_at:
            start = datetime.fromisoformat(self.started_at)
            end = datetime.fromisoformat(self.completed_at) if self.completed_at else datetime.now(timezone.utc)
            total_elapsed_sec = max(0.0, (end - start).total_seconds())

        return {
            "job_id": self.job_id,
            "filename": self.filename,
            "book_title": self.book_title,
            "category": self.category,
            "domain": self.domain,
            "size_bytes": self.size_bytes,
            "status": self.status,
            "stage": self.stage,
            "progress": round(self.progress, 2),
            "stage_progress": round(self.stage_progress, 2),
            "progress_basis": "weighted_pipeline_estimate; stage progress uses downstream work counters",
            "message": self.message,
            "eta_sec": None if self.eta_sec is None else round(self.eta_sec, 1),
            "total_elapsed_sec": None if total_elapsed_sec is None else round(total_elapsed_sec, 3),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "downstream_job_id": self.downstream_job_id,
            "timings": timings,
            "metrics": self.metrics,
            "error": self.error,
            "result": self.result,
        }


class JobManager:
    def __init__(self) -> None:
        self.jobs: dict[str, JobState] = {}
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.subscribers: dict[str, set[asyncio.Queue[dict[str, Any]]]] = {}
        self.worker_tasks: list[asyncio.Task[Any]] = []
        self._io_lock = asyncio.Lock()

    async def start(self) -> None:
        DATA_ROOT.mkdir(parents=True, exist_ok=True)
        for idx in range(JOB_WORKERS):
            self.worker_tasks.append(asyncio.create_task(self.worker(idx), name=f"job-worker-{idx}"))

    async def stop(self) -> None:
        for task in self.worker_tasks:
            task.cancel()
        if self.worker_tasks:
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)

    def job_dir(self, job_id: str) -> Path:
        return DATA_ROOT / job_id

    def state_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "job.json"

    async def persist(self, job: JobState) -> None:
        job.updated_at = utcnow()
        path = self.state_path(job.job_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(job.as_dict(), ensure_ascii=False, indent=2)
        async with self._io_lock:
            tmp = path.with_suffix(".tmp")
            tmp.write_text(payload, encoding="utf-8")
            tmp.replace(path)

    async def emit(self, job: JobState, event_type: str = "progress", extra: dict[str, Any] | None = None) -> None:
        event = {"type": event_type, "ts": utcnow(), "job": job.as_dict()}
        if extra:
            event.update(extra)
        events_path = self.job_dir(job.job_id) / "events.jsonl"
        async with self._io_lock:
            with events_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(event, ensure_ascii=False) + "\n")
        for queue in list(self.subscribers.get(job.job_id, set())):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass

    async def set_stage(
        self,
        job: JobState,
        stage: str,
        *,
        message: str | None = None,
        local_progress: float | None = None,
        downstream_progress: float | None = None,
        eta_sec: float | None = None,
        emit: bool = True,
    ) -> None:
        now = time.monotonic()
        previous = job.stage
        if previous and previous != stage and previous in job.timings:
            await self.finish_stage(job, previous)

        if stage not in job.timings:
            job.timings[stage] = empty_timing()
        current = job.timings[stage]
        if current.get("status") == "waiting":
            current["status"] = "running"
            current["started_at"] = utcnow()
            current["_started_mono"] = now

        job.stage = stage
        job.status = "running" if stage not in {"completed", "failed"} else stage
        if message:
            job.message = message
        fraction_pct = local_progress if local_progress is not None else downstream_progress
        job.stage_progress = clamp(float(fraction_pct or 0.0), 0.0, 100.0)
        job.progress = self.calculate_overall_progress(stage, job.stage_progress)
        if eta_sec is not None:
            job.eta_sec = max(0.0, float(eta_sec))
        await self.persist(job)
        if emit:
            await self.emit(job)

    def calculate_overall_progress(self, stage: str, stage_progress: float) -> float:
        if stage in {"completed", "failed"}:
            return 100.0
        completed_weight = 0.0
        for name in STAGE_ORDER:
            if name == stage:
                break
            completed_weight += STAGE_WEIGHTS.get(name, 0.0)
        stage_weight = STAGE_WEIGHTS.get(stage, 0.0)
        fraction = clamp(stage_progress / 100.0, 0.0, 1.0)
        return min(99.5, 100.0 * (completed_weight + stage_weight * fraction))

    async def finish_stage(self, job: JobState, stage: str, status: str = "completed") -> None:
        timing = job.timings.get(stage)
        if not timing or timing.get("status") != "running":
            return
        now = time.monotonic()
        timing["status"] = status
        timing["finished_at"] = utcnow()
        timing["elapsed_sec"] = round(now - float(timing.get("_started_mono", now)), 3)
        timing.pop("_started_mono", None)

    async def create(
        self,
        upload: UploadFile,
        *,
        book_title: str,
        category: str,
        domain: str,
    ) -> JobState:
        job_id = str(uuid.uuid4())
        job_dir = self.job_dir(job_id)
        input_dir = job_dir / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        dest = input_dir / "book.pdf"
        upload_started = time.monotonic()
        size = 0
        with dest.open("wb") as out:
            while True:
                block = await upload.read(1024 * 1024)
                if not block:
                    break
                size += len(block)
                if size > MAX_UPLOAD_MB * 1024 * 1024:
                    out.close()
                    shutil.rmtree(job_dir, ignore_errors=True)
                    raise HTTPException(status_code=413, detail=f"File exceeds MAX_UPLOAD_MB={MAX_UPLOAD_MB}")
                out.write(block)

        filename = upload.filename or "book.pdf"
        resolved_title = book_title.strip() or Path(filename).stem
        job = JobState(
            job_id=job_id,
            filename=filename,
            size_bytes=size,
            book_title=resolved_title,
            category=category.strip(),
            domain=domain.strip() or SFT_DEFAULT_DOMAIN,
        )
        job.timings["upload"] = {
            "started_at": job.created_at,
            "finished_at": utcnow(),
            "elapsed_sec": round(time.monotonic() - upload_started, 3),
            "status": "completed",
        }
        self.jobs[job_id] = job
        await self.persist(job)
        await self.emit(job, "job_created")
        await self.queue.put(job_id)
        return job

    async def worker(self, worker_id: int) -> None:
        while True:
            job_id = await self.queue.get()
            try:
                job = self.jobs[job_id]
                await self.run_pipeline(job, worker_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                job = self.jobs.get(job_id)
                if job:
                    await self.fail(job, "ORCHESTRATOR_EXCEPTION", f"{type(exc).__name__}: {exc}")
            finally:
                self.queue.task_done()

    async def fail(self, job: JobState, code: str, details: str, downstream: dict[str, Any] | None = None) -> None:
        await self.finish_stage(job, job.stage, "failed")
        job.status = "failed"
        job.stage = "failed"
        job.stage_progress = 100.0
        job.progress = 100.0
        job.message = "Pipeline failed"
        job.completed_at = utcnow()
        job.eta_sec = 0.0
        job.error = {"code": code, "message": "Pipeline failed", "details": details, "downstream": downstream}
        await self.persist(job)
        await self.emit(job, "failed")

    async def run_pipeline(self, job: JobState, worker_id: int) -> None:
        job.started_at = utcnow()
        job.metrics["worker_id"] = worker_id
        job.metrics["pipeline"] = {"chunker": {}, "sft": {}, "pdf": {}}
        await self.run_chunker(job)
        if job.status == "failed":
            return
        await self.run_sft(job)
        if job.status == "failed":
            return
        await self.run_review_pdf(job)
        if job.status == "failed":
            return
        await self.complete(job)

    async def run_chunker(self, job: JobState) -> None:
        await self.set_stage(job, "chunker_submit", message="Submitting PDF to Chunker API", local_progress=0)
        pdf_path = self.job_dir(job.job_id) / "input" / "book.pdf"
        timeout = httpx.Timeout(CHUNKER_TIMEOUT_SEC, connect=30.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            with pdf_path.open("rb") as fh:
                response = await client.post(
                    f"{CHUNKER_BASE_URL}/v1/jobs",
                    files={"pdf_file": (job.filename, fh, "application/pdf")},
                )
            if response.status_code >= 400:
                await self.fail(job, "CHUNKER_SUBMIT_FAILED", response.text)
                return
            submit = response.json()
            downstream_job_id = submit.get("job_id")
            if not downstream_job_id:
                await self.fail(job, "CHUNKER_BAD_RESPONSE", json.dumps(submit, ensure_ascii=False))
                return
            job.downstream_job_id = downstream_job_id
            job.metrics["pipeline"]["chunker"]["job_id"] = downstream_job_id
            await self.finish_stage(job, "chunker_submit")
            await self.persist(job)

            last_stage: str | None = None
            while True:
                status_resp = await client.get(f"{CHUNKER_BASE_URL}/v1/jobs/{downstream_job_id}")
                if status_resp.status_code >= 400:
                    await self.fail(job, "CHUNKER_STATUS_FAILED", status_resp.text)
                    return
                downstream = status_resp.json()
                ds_status = str(downstream.get("status", "running"))
                ds_stage = str(downstream.get("stage") or "chunking")
                try:
                    ds_progress = float(downstream.get("progress")) if downstream.get("progress") is not None else 0.0
                except Exception:
                    ds_progress = 0.0
                mapped_stage = CHUNKER_STAGE_MAP.get(ds_stage, "chunking")
                last_stage = mapped_stage
                await self.set_stage(
                    job,
                    mapped_stage,
                    message=str(downstream.get("message") or f"Chunker: {ds_stage}"),
                    downstream_progress=ds_progress,
                )
                job.metrics["pipeline"]["chunker"].update({
                    "status": ds_status,
                    "stage": ds_stage,
                    "progress": ds_progress,
                    "updated_at": utcnow(),
                })
                if ds_status == "failed":
                    await self.fail(job, "CHUNKER_FAILED", "Downstream Chunker job failed", downstream=downstream)
                    return
                if ds_status == "completed":
                    break
                await asyncio.sleep(POLL_INTERVAL_SEC)

            if last_stage:
                await self.finish_stage(job, last_stage)
            await self.set_stage(job, "chunker_artifacts", message="Downloading Chunker artifacts", local_progress=0)
            artifacts = self.job_dir(job.job_id) / "artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            chunks_ok = await self.download_chunker_artifact(client, downstream_job_id, "chunks.jsonl", artifacts / "chunks.jsonl")
            if not chunks_ok:
                await self.fail(job, "CHUNKER_ARTIFACT_MISSING", "Chunker completed but chunks.jsonl was not available")
                return
            await self.set_stage(job, "chunker_artifacts", message="Downloading Chunker artifacts", local_progress=55)
            await self.download_chunker_artifact(client, downstream_job_id, "ocr.json", artifacts / "ocr.json")
            await self.set_stage(job, "chunker_artifacts", message="Downloading Chunker artifacts", local_progress=80)
            await self.download_chunker_artifact(client, downstream_job_id, "chunker.log", artifacts / "chunker.log")
            await self.finish_stage(job, "chunker_artifacts")

        chunks = jsonl_rows(self.job_dir(job.job_id) / "artifacts" / "chunks.jsonl")
        if not chunks:
            await self.fail(job, "EMPTY_CHUNKER_OUTPUT", "chunks.jsonl contains no chunks")
            return
        job.metrics["pipeline"]["chunker"]["chunk_count"] = len(chunks)
        await self.persist(job)

    async def download_chunker_artifact(self, client: httpx.AsyncClient, downstream_job_id: str, name: str, dest: Path) -> bool:
        response = await client.get(f"{CHUNKER_BASE_URL}/v1/jobs/{downstream_job_id}/download/{name}")
        if response.status_code == 200:
            dest.write_bytes(response.content)
            return True
        return False

    def _neighbor_payload(self, chunk: dict[str, Any] | None) -> dict[str, Any] | None:
        if not chunk:
            return None
        return {
            "chunk_id": first_text(chunk.get("id"), chunk.get("original_id"), "chunk"),
            "content": first_text(chunk.get("text")),
            "doc": first_text(chunk.get("doc")),
            "source_file": first_text(chunk.get("src"), chunk.get("doc")),
            "metadata": {
                "source_doc": chunk.get("doc"),
                "source_title": chunk.get("src"),
                "section": chunk.get("sec"),
                "ps": chunk.get("ps"),
                "pe": chunk.get("pe"),
                "start": chunk.get("start"),
                "end": chunk.get("end"),
            },
        }

    def build_sft_payload(
        self,
        job: JobState,
        chunks: list[dict[str, Any]],
        index: int,
        telemetry_run_id: str,
    ) -> dict[str, Any]:
        chunk = chunks[index]
        chunk_id = first_text(chunk.get("id"), chunk.get("original_id"), f"chunk_{index:06d}")
        text = first_text(chunk.get("text"))
        metadata = {
            "input_kind": "book_chunk",
            "source_doc": chunk.get("doc"),
            "source_title": job.book_title or chunk.get("src") or job.filename,
            "category": job.category,
            "section": chunk.get("sec"),
            "ps": chunk.get("ps"),
            "pe": chunk.get("pe"),
            "page_labels": chunk.get("page_labels"),
            "start": chunk.get("start"),
            "end": chunk.get("end"),
            "original_index": index,
            "book2sft_job_id": job.job_id,
            "telemetry_run_id": telemetry_run_id,
        }
        return {
            "mode": "instruction",
            "text": text,
            "chunk_id": chunk_id,
            "source_file": job.filename,
            "source_language": SFT_SOURCE_LANGUAGE,
            "target_language": SFT_TARGET_LANGUAGE,
            "domain": job.domain,
            "num_variations_per_task": 1,
            "max_pairs": SFT_MAX_PAIRS_PER_CHUNK,
            "pair_concurrency": SFT_PAIR_CONCURRENCY,
            "previous_chunk": self._neighbor_payload(chunks[index - 1] if index > 0 else None),
            "next_chunk": self._neighbor_payload(chunks[index + 1] if index + 1 < len(chunks) else None),
            "enable_context_expansion": True,
            "standalone_sft": True,
            "metadata": metadata,
        }

    async def monitor_generator_telemetry(
        self,
        job: JobState,
        chunk_id: str,
        run_id: str,
        stop_event: asyncio.Event,
    ) -> None:
        last_signature: tuple[Any, ...] | None = None
        async with httpx.AsyncClient(timeout=8.0) as client:
            while not stop_event.is_set():
                try:
                    response = await client.get(f"{SFT_GENERATOR_BASE_URL}/telemetry/{run_id}")
                    if response.status_code == 200:
                        snapshot = response.json()
                        if snapshot.get("status") != "not_found":
                            progress = snapshot.get("progress") if isinstance(snapshot.get("progress"), dict) else {}
                            signature = (
                                snapshot.get("status"),
                                progress.get("stage"),
                                progress.get("completed"),
                                progress.get("total"),
                                progress.get("percent"),
                            )
                            if signature != last_signature:
                                last_signature = signature
                                sft = job.metrics.setdefault("pipeline", {}).setdefault("sft", {})
                                active = sft.setdefault("active", {})
                                active[chunk_id] = {
                                    "telemetry_run_id": run_id,
                                    "status": snapshot.get("status"),
                                    "progress": progress,
                                    "timings": snapshot.get("timings", {}),
                                    "metrics": snapshot.get("metrics", {}),
                                    "updated_at": utcnow(),
                                }
                                await self.update_sft_stage(job, emit=True)
                except Exception:
                    # Telemetry must not change SFT generation behavior.
                    pass
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=SFT_TELEMETRY_POLL_SEC)
                except asyncio.TimeoutError:
                    pass

    async def update_sft_stage(self, job: JobState, *, emit: bool) -> None:
        sft = job.metrics.setdefault("pipeline", {}).setdefault("sft", {})
        total = max(1, int(sft.get("total_chunks") or 0))
        completed = int(sft.get("completed_chunks") or 0)
        active = sft.get("active") if isinstance(sft.get("active"), dict) else {}
        active_fraction = 0.0
        targets_completed = 0
        targets_total = 0
        current_targets: list[dict[str, Any]] = []
        for chunk_id, info in active.items():
            progress = info.get("progress") if isinstance(info.get("progress"), dict) else {}
            try:
                pct = clamp(float(progress.get("percent") or 0.0), 0.0, 100.0)
            except Exception:
                pct = 0.0
            # Generator telemetry covers planning + target generation. The /run
            # quality-evaluation tail is intentionally not guessed; active work is
            # capped at 80% until the whole chunk call completes.
            active_fraction += 0.80 * (pct / 100.0)
            try:
                pc = int(progress.get("completed") or 0)
                pt = int(progress.get("total") or 0)
            except Exception:
                pc, pt = 0, 0
            targets_completed += pc
            targets_total += pt
            current_targets.append({
                "chunk_id": chunk_id,
                "stage": progress.get("stage"),
                "completed": pc,
                "total": pt,
                "percent": pct,
                "target_id": progress.get("target_id"),
                "task_type": progress.get("task_type"),
                "generated_pairs_so_far": progress.get("generated_pairs_so_far"),
            })
        stage_pct = clamp(((completed + active_fraction) / total) * 100.0, 0.0, 99.0 if completed < total else 100.0)

        timing = job.timings.get("sft_generation", {})
        stage_elapsed = 0.0
        if timing.get("_started_mono") is not None:
            stage_elapsed = max(0.0, time.monotonic() - float(timing["_started_mono"]))
        eta: float | None = None
        if completed > 0 and stage_elapsed > 0:
            throughput = completed / stage_elapsed
            eta = max(0.0, (total - completed) / throughput) if throughput > 0 else None

        sft["targets_live"] = {"completed": targets_completed, "total": targets_total}
        sft["current_targets"] = current_targets[: max(1, SFT_CONCURRENCY)]
        sft["active_chunks"] = len(active)
        message = f"SFT generation: {completed}/{total} chunks"
        if targets_total:
            message += f" | live targets {targets_completed}/{targets_total}"
        await self.set_stage(
            job,
            "sft_generation",
            message=message,
            local_progress=stage_pct,
            eta_sec=eta,
            emit=emit,
        )

    async def run_one_sft_chunk(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        job: JobState,
        chunks: list[dict[str, Any]],
        index: int,
    ) -> tuple[int, dict[str, Any], float]:
        chunk = chunks[index]
        chunk_id = first_text(chunk.get("id"), chunk.get("original_id"), f"chunk_{index:06d}")
        telemetry_run_id = f"{job.job_id}-{index:06d}-{uuid.uuid4().hex[:8]}"
        payload = self.build_sft_payload(job, chunks, index, telemetry_run_id)
        async with semaphore:
            started = time.perf_counter()
            stop_event = asyncio.Event()
            monitor = asyncio.create_task(
                self.monitor_generator_telemetry(job, chunk_id, telemetry_run_id, stop_event),
                name=f"telemetry-{index}",
            )
            result: dict[str, Any]
            try:
                result = {"error": True, "message": "request_not_started"}
                for attempt in range(SFT_REQUEST_RETRIES + 1):
                    try:
                        response = await client.post(
                            f"{SFT_ORCHESTRATOR_BASE_URL}/run",
                            json=payload,
                            timeout=SFT_TIMEOUT_SEC,
                        )
                        if response.status_code >= 400:
                            result = {
                                "error": True,
                                "status_code": response.status_code,
                                "body": response.text[:8000],
                            }
                        else:
                            result = response.json()
                            break
                    except Exception as exc:
                        result = {"error": True, "exception": f"{type(exc).__name__}: {exc}"}
                    if attempt < SFT_REQUEST_RETRIES:
                        await asyncio.sleep(min(8.0, 1.5 * (attempt + 1)))
            finally:
                stop_event.set()
                await asyncio.gather(monitor, return_exceptions=True)
                sft = job.metrics.setdefault("pipeline", {}).setdefault("sft", {})
                active = sft.setdefault("active", {})
                active.pop(chunk_id, None)
            elapsed = round(time.perf_counter() - started, 3)
            result["_book2sft"] = {
                "chunk_id": chunk_id,
                "chunk_index": index,
                "elapsed_sec": elapsed,
                "telemetry_run_id": telemetry_run_id,
            }
            return index, result, elapsed

    def aggregate_sft_result(self, sft_metrics: dict[str, Any], result: dict[str, Any]) -> None:
        timings = result.get("timings") if isinstance(result.get("timings"), dict) else {}
        timing_sum = sft_metrics.setdefault("timings_sum_sec", {})
        for key, value in nested_numeric_timings(timings).items():
            timing_sum[key] = round(float(timing_sum.get(key, 0.0)) + float(value), 3)
        telemetry = result.get("telemetry") if isinstance(result.get("telemetry"), dict) else {}
        for prefix in ("instruction_value_assessment", "instruction_generation", "candidate_processing"):
            part = telemetry.get(prefix) if isinstance(telemetry.get(prefix), dict) else {}
            for key, value in nested_numeric_timings(part, f"telemetry.{prefix}").items():
                timing_sum[key] = round(float(timing_sum.get(key, 0.0)) + float(value), 3)
        sft_metrics["generated_pairs"] = int(sft_metrics.get("generated_pairs") or 0) + int(result.get("generated_pairs_count") or 0)
        sft_metrics["quality_pass_pairs"] = int(sft_metrics.get("quality_pass_pairs") or 0) + int(result.get("quality_pass_count") or 0)
        sft_metrics["per_chunk_selected_pairs"] = int(sft_metrics.get("per_chunk_selected_pairs") or 0) + int(result.get("keep_count") or 0)

    def quality_candidates(self, raw_results: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        candidates: list[dict[str, Any]] = []
        all_pairs: list[dict[str, Any]] = []
        for result in raw_results:
            if result.get("error"):
                continue
            run_meta = result.get("_book2sft") if isinstance(result.get("_book2sft"), dict) else {}
            for pair in result.get("final_pairs", []) or []:
                if not isinstance(pair, dict):
                    continue
                cloned = copy.deepcopy(pair)
                cloned.setdefault("book2sft", {}).update({
                    "source_chunk_id": run_meta.get("chunk_id"),
                    "source_chunk_index": run_meta.get("chunk_index"),
                    "job_id": None,
                })
                all_pairs.append(cloned)
                quality = cloned.get("quality_decision")
                if quality is None:
                    quality = cloned.get("final_decision")
                if quality == "accept":
                    cloned["quality_decision"] = "accept"
                    cloned["final_decision"] = "accept"
                    cloned["selected_for_dataset"] = False
                    source_pair = cloned.setdefault("source_pair", {})
                    if isinstance(source_pair, dict):
                        source_pair.setdefault("source_chunk_id", run_meta.get("chunk_id"))
                    candidates.append(copy.deepcopy(cloned))
        return candidates, all_pairs

    async def run_sft(self, job: JobState) -> None:
        artifacts = self.job_dir(job.job_id) / "artifacts"
        chunks_path = artifacts / "chunks.jsonl"
        chunks = jsonl_rows(chunks_path)
        await self.set_stage(job, "sft_prepare", message=f"Preparing {len(chunks)} chunks for SFT", local_progress=0)
        if not chunks:
            await self.fail(job, "SFT_NO_CHUNKS", "No chunks available for SFT generation")
            return

        sft_dir = self.job_dir(job.job_id) / "sft"
        sft_dir.mkdir(parents=True, exist_ok=True)
        sft_metrics = job.metrics.setdefault("pipeline", {}).setdefault("sft", {})
        sft_metrics.update({
            "total_chunks": len(chunks),
            "completed_chunks": 0,
            "failed_chunks": 0,
            "rejected_chunks": 0,
            "active": {},
            "generated_pairs": 0,
            "quality_pass_pairs": 0,
            "selected_pairs": 0,
            "sft_concurrency": SFT_CONCURRENCY,
            "pair_concurrency": SFT_PAIR_CONCURRENCY,
            "max_pairs_per_chunk": SFT_MAX_PAIRS_PER_CHUNK,
        })
        await self.finish_stage(job, "sft_prepare")
        await self.set_stage(job, "sft_generation", message=f"SFT generation: 0/{len(chunks)} chunks", local_progress=0)

        limits = httpx.Limits(max_connections=max(20, SFT_CONCURRENCY * 3), max_keepalive_connections=max(10, SFT_CONCURRENCY * 2))
        timeout = httpx.Timeout(SFT_TIMEOUT_SEC, connect=30.0)
        semaphore = asyncio.Semaphore(SFT_CONCURRENCY)
        raw_results_by_index: dict[int, dict[str, Any]] = {}
        errors: list[dict[str, Any]] = []
        generation_started = time.monotonic()

        async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
            tasks = [
                asyncio.create_task(self.run_one_sft_chunk(client, semaphore, job, chunks, i), name=f"sft-{i}")
                for i in range(len(chunks))
            ]
            for task in asyncio.as_completed(tasks):
                index, result, elapsed = await task
                raw_results_by_index[index] = result
                sft_metrics["completed_chunks"] = int(sft_metrics.get("completed_chunks") or 0) + 1
                if result.get("error"):
                    sft_metrics["failed_chunks"] = int(sft_metrics.get("failed_chunks") or 0) + 1
                    errors.append({"chunk_index": index, "chunk_id": result.get("_book2sft", {}).get("chunk_id"), "result": result})
                else:
                    if result.get("final_decision") == "reject":
                        sft_metrics["rejected_chunks"] = int(sft_metrics.get("rejected_chunks") or 0) + 1
                    self.aggregate_sft_result(sft_metrics, result)
                sft_metrics["last_completed_chunk"] = {
                    "index": index,
                    "chunk_id": result.get("_book2sft", {}).get("chunk_id"),
                    "elapsed_sec": elapsed,
                    "error": bool(result.get("error")),
                }
                await self.update_sft_stage(job, emit=True)

        await self.finish_stage(job, "sft_generation")
        sft_metrics["generation_wall_sec"] = round(time.monotonic() - generation_started, 3)
        raw_results = [raw_results_by_index[i] for i in sorted(raw_results_by_index)]
        write_jsonl(sft_dir / "sft_raw_results.jsonl", raw_results)
        write_jsonl(sft_dir / "sft_errors.jsonl", errors)

        if int(sft_metrics.get("failed_chunks") or 0) >= len(chunks):
            await self.fail(job, "SFT_ALL_CHUNKS_FAILED", "Every SFT /run request failed")
            return

        candidates, all_pairs = self.quality_candidates(raw_results)
        for pair in all_pairs:
            pair.setdefault("book2sft", {})["job_id"] = job.job_id
        for pair in candidates:
            pair.setdefault("book2sft", {})["job_id"] = job.job_id
        write_jsonl(sft_dir / "all_evaluated_pairs.jsonl", all_pairs)
        write_jsonl(sft_dir / "quality_pass_candidates.jsonl", candidates)

        await self.set_stage(job, "sft_book_selection", message=f"Book-level selection from {len(candidates)} quality candidates", local_progress=5)
        selected_pairs: list[dict[str, Any]] = []
        selection_response: dict[str, Any] = {}
        selection_started = time.monotonic()
        if candidates:
            book_budget = SFT_BOOK_MAX_PAIRS if SFT_BOOK_MAX_PAIRS > 0 else max(1, len(chunks) * SFT_MAX_PAIRS_PER_CHUNK)
            payload = {
                "book_id": job.book_title or job.filename,
                "pairs": candidates,
                "max_pairs": book_budget,
                "max_per_concept": SFT_MAX_PER_CONCEPT,
            }
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(f"{SFT_ORCHESTRATOR_BASE_URL}/select_instruction_book", json=payload)
            if response.status_code >= 400:
                await self.fail(job, "SFT_BOOK_SELECTION_FAILED", response.text)
                return
            selection_response = response.json()
            selected_pairs = selection_response.get("selected_pairs", []) or []
        else:
            selection_response = {
                "book_id": job.book_title or job.filename,
                "quality_candidate_count": 0,
                "selected_count": 0,
                "selected_pairs": [],
                "reason": "no_quality_pass_candidates",
            }
        sft_metrics["book_selection_wall_sec"] = round(time.monotonic() - selection_started, 3)
        sft_metrics["selected_pairs"] = len(selected_pairs)
        write_json(sft_dir / "sft_book_selection.json", selection_response)
        write_jsonl(sft_dir / "sft_pairs.jsonl", selected_pairs)
        await self.set_stage(job, "sft_book_selection", message=f"Selected {len(selected_pairs)} SFT pairs", local_progress=100)
        await self.finish_stage(job, "sft_book_selection")

        await self.set_stage(job, "sft_finalize", message="Building enriched and Axolotl SFT artifacts", local_progress=10)
        chunk_index: dict[str, dict[str, Any]] = {}
        for chunk in chunks:
            for key in ("id", "original_id", "chunk_id"):
                if chunk.get(key):
                    chunk_index[str(chunk[key])] = chunk
        enriched: list[dict[str, Any]] = []
        axolotl: list[dict[str, Any]] = []
        for pair in selected_pairs:
            chunk_id = source_chunk_id(pair)
            source_chunk = chunk_index.get(chunk_id, {})
            enriched.append({"sft": pair, "chunk": source_chunk})
            question, answer = pair_question_answer(pair)
            if question and answer:
                axolotl.append({
                    "messages": [
                        {"role": "user", "content": question},
                        {"role": "assistant", "content": answer},
                    ],
                    "metadata": {
                        "book": job.book_title,
                        "category": job.category,
                        "chunk_id": chunk_id,
                        "page_start": source_chunk.get("ps"),
                        "page_end": source_chunk.get("pe"),
                        "page_labels": source_chunk.get("page_labels"),
                        "section": source_chunk.get("sec"),
                    },
                })
        write_jsonl(sft_dir / "sft_pairs_with_chunks.jsonl", enriched)
        await self.set_stage(job, "sft_finalize", message="Building enriched and Axolotl SFT artifacts", local_progress=60)
        write_jsonl(sft_dir / "sft_axolotl.jsonl", axolotl)
        write_json(sft_dir / "sft_metrics.json", sft_metrics)
        await self.set_stage(job, "sft_finalize", message="SFT artifacts finalized", local_progress=100)
        await self.finish_stage(job, "sft_finalize")
        await self.persist(job)

    async def run_pdf_command(self, args: list[str], log_path: Path) -> tuple[bool, str]:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout=1800)
        except asyncio.TimeoutError:
            process.kill()
            await process.communicate()
            log_path.write_text("PDF generation timed out\n", encoding="utf-8")
            return False, "PDF generation timed out"
        text = (stdout or b"").decode("utf-8", errors="replace")
        log_path.write_text(text, encoding="utf-8")
        return process.returncode == 0, text[-4000:]

    async def run_review_pdf(self, job: JobState) -> None:
        await self.set_stage(job, "review_pdf", message="Generating SFT review PDF", local_progress=0)
        if not PDF_SCRIPT.exists():
            await self.fail(job, "PDF_TOOL_MISSING", f"Review PDF script not found: {PDF_SCRIPT}")
            return
        base = self.job_dir(job.job_id)
        artifacts = base / "artifacts"
        sft_dir = base / "sft"
        pdf_dir = base / "review"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        selected = sft_dir / "sft_pairs.jsonl"
        all_pairs = sft_dir / "all_evaluated_pairs.jsonl"
        chunks = artifacts / "chunks.jsonl"
        book_pdf = base / "input" / "book.pdf"
        selected_pdf = pdf_dir / "review_selected.pdf"
        all_pdf = pdf_dir / "review_all_evaluated.pdf"

        selected_count = len(jsonl_rows(selected))
        all_count = len(jsonl_rows(all_pairs))
        pdf_metrics = job.metrics.setdefault("pipeline", {}).setdefault("pdf", {})
        pdf_metrics["selected_pairs"] = selected_count
        pdf_metrics["all_evaluated_pairs"] = all_count
        if selected_count == 0:
            pdf_metrics["selected_pdf_skipped"] = "no_selected_pairs"
            if all_count == 0:
                pdf_metrics["all_pdf_skipped"] = "no_evaluated_pairs"
                await self.set_stage(job, "review_pdf", message="No SFT pairs available for PDF", local_progress=100)
                await self.finish_stage(job, "review_pdf")
                return
        common = [
            sys.executable,
            str(PDF_SCRIPT),
            "--chunks", str(chunks),
            "--book-pdf", str(book_pdf),
            "--book-title", job.book_title,
            "--language", "fa",
            "--include-reference-text",
        ]
        if selected_count > 0:
            ok, tail = await self.run_pdf_command(
                [*common, "--pairs", str(selected), "--output", str(selected_pdf)],
                pdf_dir / "review_selected.log",
            )
            pdf_metrics["selected_pdf_ok"] = ok
            if not ok:
                await self.fail(job, "SELECTED_PDF_FAILED", tail)
                return
        await self.set_stage(job, "review_pdf", message="Generating audit PDF", local_progress=55)
        if all_count > 0:
            ok, tail = await self.run_pdf_command(
                [*common, "--pairs", str(all_pairs), "--output", str(all_pdf), "--include-rejected"],
                pdf_dir / "review_all_evaluated.log",
            )
            pdf_metrics["all_pdf_ok"] = ok
            if not ok:
                await self.fail(job, "AUDIT_PDF_FAILED", tail)
                return
        await self.set_stage(job, "review_pdf", message="Review PDFs generated", local_progress=100)
        await self.finish_stage(job, "review_pdf")

    async def complete(self, job: JobState) -> None:
        base = self.job_dir(job.job_id)
        artifacts = base / "artifacts"
        sft_dir = base / "sft"
        review = base / "review"
        chunks = jsonl_rows(artifacts / "chunks.jsonl")
        selected = jsonl_rows(sft_dir / "sft_pairs.jsonl")
        all_pairs = jsonl_rows(sft_dir / "all_evaluated_pairs.jsonl")
        sft_metrics = job.metrics.get("pipeline", {}).get("sft", {})
        warnings: list[str] = []
        failed_chunks = int(sft_metrics.get("failed_chunks") or 0)
        if failed_chunks:
            warnings.append(f"{failed_chunks} SFT chunk request(s) failed; see sft_errors.jsonl")
        if not selected:
            warnings.append("No SFT pair survived book-level selection")
        job.status = "completed"
        job.stage = "completed"
        job.stage_progress = 100.0
        job.progress = 100.0
        job.message = f"Completed: {len(chunks)} chunks → {len(selected)} selected SFT pairs"
        job.completed_at = utcnow()
        job.eta_sec = 0.0
        job.result = {
            "chunk_count": len(chunks),
            "all_evaluated_pairs": len(all_pairs),
            "selected_sft_pairs": len(selected),
            "warnings": warnings,
            "downloads": {
                "chunks_jsonl": f"/v1/jobs/{job.job_id}/download/chunks.jsonl",
                "ocr_json": f"/v1/jobs/{job.job_id}/download/ocr.json",
                "chunker_log": f"/v1/jobs/{job.job_id}/download/chunker.log",
                "sft_jsonl": f"/v1/jobs/{job.job_id}/download/sft_pairs.jsonl",
                "sft_with_chunks_jsonl": f"/v1/jobs/{job.job_id}/download/sft_pairs_with_chunks.jsonl",
                "sft_axolotl_jsonl": f"/v1/jobs/{job.job_id}/download/sft_axolotl.jsonl",
                "all_evaluated_jsonl": f"/v1/jobs/{job.job_id}/download/all_evaluated_pairs.jsonl",
                "sft_errors_jsonl": f"/v1/jobs/{job.job_id}/download/sft_errors.jsonl",
                "sft_metrics_json": f"/v1/jobs/{job.job_id}/download/sft_metrics.json",
                "review_pdf": f"/v1/jobs/{job.job_id}/download/review_selected.pdf" if (review / "review_selected.pdf").exists() else None,
                "review_all_pdf": f"/v1/jobs/{job.job_id}/download/review_all_evaluated.pdf" if (review / "review_all_evaluated.pdf").exists() else None,
            },
        }
        await self.persist(job)
        await self.emit(job, "completed")

    def get(self, job_id: str) -> JobState:
        job = self.jobs.get(job_id)
        if job:
            return job
        state_path = self.state_path(job_id)
        if not state_path.exists():
            raise HTTPException(status_code=404, detail="Job not found")
        data = json.loads(state_path.read_text(encoding="utf-8"))
        job = JobState(
            job_id=data["job_id"],
            filename=data["filename"],
            size_bytes=data["size_bytes"],
            book_title=data.get("book_title", ""),
            category=data.get("category", ""),
            domain=data.get("domain", SFT_DEFAULT_DOMAIN),
        )
        for key in [
            "created_at", "updated_at", "started_at", "completed_at", "status", "stage",
            "progress", "stage_progress", "message", "eta_sec", "error", "result", "downstream_job_id",
            "timings", "metrics",
        ]:
            if key in data:
                setattr(job, key, data[key])
        self.jobs[job_id] = job
        return job

    async def subscribe(self, job_id: str) -> AsyncIterator[dict[str, Any]]:
        self.get(job_id)
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=200)
        self.subscribers.setdefault(job_id, set()).add(queue)
        try:
            yield {"type": "snapshot", "ts": utcnow(), "job": self.get(job_id).as_dict()}
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield event
                except asyncio.TimeoutError:
                    yield {"type": "ping", "ts": utcnow()}
        finally:
            self.subscribers.get(job_id, set()).discard(queue)


manager = JobManager()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await manager.start()
    try:
        yield
    finally:
        await manager.stop()


app = FastAPI(title="Book2SFT", version="0.2.0-full-pipeline", lifespan=lifespan)
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


async def check_service(url: str) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url)
        return {"ok": response.status_code == 200, "status_code": response.status_code}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "book2sft",
        "version": "0.2.0-full-pipeline",
        "job_workers": JOB_WORKERS,
        "sft_concurrency": SFT_CONCURRENCY,
        "sft_pair_concurrency": SFT_PAIR_CONCURRENCY,
        "chunker_base_url": CHUNKER_BASE_URL,
        "sft_orchestrator_base_url": SFT_ORCHESTRATOR_BASE_URL,
        "sft_generator_base_url": SFT_GENERATOR_BASE_URL,
    }


@app.get("/ready")
async def ready() -> JSONResponse:
    chunker, orchestrator, generator = await asyncio.gather(
        check_service(f"{CHUNKER_BASE_URL}/health"),
        check_service(f"{SFT_ORCHESTRATOR_BASE_URL}/health"),
        check_service(f"{SFT_GENERATOR_BASE_URL}/health"),
    )
    ok = bool(chunker.get("ok") and orchestrator.get("ok") and generator.get("ok"))
    return JSONResponse(
        {
            "status": "ready" if ok else "not_ready",
            "services": {"chunker": chunker, "sft_orchestrator": orchestrator, "sft_generator": generator},
        },
        status_code=200 if ok else 503,
    )


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse((FRONTEND_DIR / "index.html").read_text(encoding="utf-8"))


@app.post("/v1/jobs", status_code=202)
async def create_job(
    pdf_file: UploadFile = File(...),
    book_title: str = Form(""),
    category: str = Form(""),
    domain: str = Form(""),
) -> dict[str, Any]:
    name = (pdf_file.filename or "").lower()
    if not name.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are accepted")
    job = await manager.create(
        pdf_file,
        book_title=book_title,
        category=category,
        domain=domain or SFT_DEFAULT_DOMAIN,
    )
    return {
        "job_id": job.job_id,
        "status": job.status,
        "status_url": f"/v1/jobs/{job.job_id}",
        "events_url": f"/v1/jobs/{job.job_id}/events",
    }


@app.get("/v1/jobs/{job_id}")
async def get_job(job_id: str) -> dict[str, Any]:
    return manager.get(job_id).as_dict()


@app.get("/v1/jobs/{job_id}/events")
async def job_events(job_id: str, request: Request) -> StreamingResponse:
    async def stream() -> AsyncIterator[str]:
        async for event in manager.subscribe(job_id):
            if await request.is_disconnected():
                break
            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
    return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})


ARTIFACTS: dict[str, tuple[str, str]] = {
    "chunks.jsonl": ("artifacts/chunks.jsonl", "application/jsonl"),
    "ocr.json": ("artifacts/ocr.json", "application/json"),
    "chunker.log": ("artifacts/chunker.log", "text/plain"),
    "sft_pairs.jsonl": ("sft/sft_pairs.jsonl", "application/jsonl"),
    "sft_pairs_with_chunks.jsonl": ("sft/sft_pairs_with_chunks.jsonl", "application/jsonl"),
    "sft_axolotl.jsonl": ("sft/sft_axolotl.jsonl", "application/jsonl"),
    "all_evaluated_pairs.jsonl": ("sft/all_evaluated_pairs.jsonl", "application/jsonl"),
    "quality_pass_candidates.jsonl": ("sft/quality_pass_candidates.jsonl", "application/jsonl"),
    "sft_raw_results.jsonl": ("sft/sft_raw_results.jsonl", "application/jsonl"),
    "sft_errors.jsonl": ("sft/sft_errors.jsonl", "application/jsonl"),
    "sft_metrics.json": ("sft/sft_metrics.json", "application/json"),
    "sft_book_selection.json": ("sft/sft_book_selection.json", "application/json"),
    "review_selected.pdf": ("review/review_selected.pdf", "application/pdf"),
    "review_all_evaluated.pdf": ("review/review_all_evaluated.pdf", "application/pdf"),
    "review_selected.log": ("review/review_selected.log", "text/plain"),
    "review_all_evaluated.log": ("review/review_all_evaluated.log", "text/plain"),
}


@app.get("/v1/jobs/{job_id}/download/{artifact}")
async def download(job_id: str, artifact: str) -> FileResponse:
    if artifact not in ARTIFACTS:
        raise HTTPException(status_code=404, detail="Artifact not found")
    manager.get(job_id)
    relative, media = ARTIFACTS[artifact]
    path = manager.job_dir(job_id) / relative
    if not path.exists():
        raise HTTPException(status_code=404, detail="Artifact not available yet")
    return FileResponse(path, filename=artifact, media_type=media)
