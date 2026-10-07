from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
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


DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data/jobs"))
CHUNKER_BASE_URL = os.getenv("CHUNKER_BASE_URL", "http://host.docker.internal:8080").rstrip("/")
CHUNKER_TIMEOUT_SEC = env_float("CHUNKER_TIMEOUT_SEC", 7200.0)
POLL_INTERVAL_SEC = max(0.25, env_float("POLL_INTERVAL_SEC", 1.0))
JOB_WORKERS = max(1, env_int("JOB_WORKERS", 2))
MAX_UPLOAD_MB = max(1, env_int("MAX_UPLOAD_MB", 500))


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
    "finalizing",
    "artifact_download",
    "completed",
]

# Coarse overall weights. The stage-local progress reported by the downstream
# service is folded into these weights; no fake ETA is generated when the
# downstream service does not expose enough information.
STAGE_WEIGHTS = {
    "upload": 0.02,
    "chunker_submit": 0.01,
    "ocr": 0.22,
    "normalizing": 0.03,
    "chunking": 0.05,
    "book_plan": 0.05,
    "atomizing": 0.28,
    "planning": 0.20,
    "validating": 0.06,
    "finalizing": 0.03,
    "artifact_download": 0.05,
}


def empty_timing() -> dict[str, Any]:
    return {
        "started_at": None,
        "finished_at": None,
        "elapsed_sec": 0.0,
        "status": "waiting",
    }


@dataclass
class JobState:
    job_id: str
    filename: str
    size_bytes: int
    created_at: str = field(default_factory=utcnow)
    updated_at: str = field(default_factory=utcnow)
    started_at: str | None = None
    completed_at: str | None = None
    status: str = "queued"
    stage: str = "queued"
    progress: float = 0.0
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
            "size_bytes": self.size_bytes,
            "status": self.status,
            "stage": self.stage,
            "progress": round(self.progress, 2),
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
        self._lock = asyncio.Lock()

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
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(job.as_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    async def emit(self, job: JobState, event_type: str = "progress", extra: dict[str, Any] | None = None) -> None:
        event = {
            "type": event_type,
            "ts": utcnow(),
            "job": job.as_dict(),
        }
        if extra:
            event.update(extra)

        events_path = self.job_dir(job.job_id) / "events.jsonl"
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
    ) -> None:
        now = time.monotonic()
        previous = job.stage

        if previous and previous != stage and previous in job.timings:
            prev_t = job.timings[previous]
            if prev_t.get("status") == "running":
                prev_t["status"] = "completed"
                prev_t["finished_at"] = utcnow()
                prev_t["elapsed_sec"] = round(now - float(prev_t.get("_started_mono", now)), 3)
                prev_t.pop("_started_mono", None)

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

        job.progress = self.calculate_overall_progress(stage, local_progress, downstream_progress)
        await self.persist(job)
        await self.emit(job)

    def calculate_overall_progress(
        self,
        stage: str,
        local_progress: float | None,
        downstream_progress: float | None,
    ) -> float:
        if stage == "completed":
            return 100.0
        if stage == "failed":
            return 100.0

        completed_weight = 0.0
        for name in STAGE_ORDER:
            if name == stage:
                break
            completed_weight += STAGE_WEIGHTS.get(name, 0.0)

        stage_weight = STAGE_WEIGHTS.get(stage, 0.0)
        fraction = 0.0
        if local_progress is not None:
            fraction = max(0.0, min(1.0, local_progress / 100.0))
        elif downstream_progress is not None:
            fraction = max(0.0, min(1.0, downstream_progress / 100.0))

        return min(99.5, 100.0 * (completed_weight + stage_weight * fraction))

    async def finish_stage(self, job: JobState, stage: str, status: str = "completed") -> None:
        timing = job.timings.get(stage)
        if not timing:
            return
        if timing.get("status") == "running":
            now = time.monotonic()
            timing["status"] = status
            timing["finished_at"] = utcnow()
            timing["elapsed_sec"] = round(now - float(timing.get("_started_mono", now)), 3)
            timing.pop("_started_mono", None)

    async def create(self, upload: UploadFile) -> JobState:
        job_id = str(uuid.uuid4())
        job_dir = self.job_dir(job_id)
        input_dir = job_dir / "input"
        input_dir.mkdir(parents=True, exist_ok=True)

        dest = input_dir / "book.pdf"
        size = 0
        with dest.open("wb") as out:
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_UPLOAD_MB * 1024 * 1024:
                    out.close()
                    shutil.rmtree(job_dir, ignore_errors=True)
                    raise HTTPException(status_code=413, detail=f"File exceeds MAX_UPLOAD_MB={MAX_UPLOAD_MB}")
                out.write(chunk)

        job = JobState(job_id=job_id, filename=upload.filename or "book.pdf", size_bytes=size)
        job.timings["upload"] = {
            "started_at": job.created_at,
            "finished_at": utcnow(),
            "elapsed_sec": 0.0,
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
                    await self.fail(job, "ORCHESTRATOR_EXCEPTION", str(exc))
            finally:
                self.queue.task_done()

    async def fail(self, job: JobState, code: str, details: str, downstream: dict[str, Any] | None = None) -> None:
        await self.finish_stage(job, job.stage, "failed")
        job.status = "failed"
        job.stage = "failed"
        job.progress = 100.0
        job.message = "Pipeline failed"
        job.completed_at = utcnow()
        job.error = {
            "code": code,
            "message": "Pipeline failed",
            "details": details,
            "downstream": downstream,
        }
        await self.persist(job)
        await self.emit(job, "failed")

    async def run_pipeline(self, job: JobState, worker_id: int) -> None:
        job.started_at = utcnow()
        job.metrics["worker_id"] = worker_id
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
            await self.finish_stage(job, "chunker_submit")
            job.metrics["chunker_submit_response"] = submit
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
                ds_progress_raw = downstream.get("progress")
                try:
                    ds_progress = float(ds_progress_raw) if ds_progress_raw is not None else None
                except Exception:
                    ds_progress = None

                mapped_stage = ds_stage if ds_stage in STAGE_WEIGHTS else "chunking"
                if mapped_stage != last_stage:
                    if last_stage:
                        await self.finish_stage(job, last_stage)
                    last_stage = mapped_stage

                await self.set_stage(
                    job,
                    mapped_stage,
                    message=str(downstream.get("message") or f"Chunker: {ds_stage}"),
                    downstream_progress=ds_progress,
                )
                job.metrics["chunker"] = {
                    "job_id": downstream_job_id,
                    "status": ds_status,
                    "stage": ds_stage,
                    "progress": ds_progress,
                    "updated_at": utcnow(),
                }
                await self.persist(job)

                if ds_status == "failed":
                    await self.fail(
                        job,
                        "CHUNKER_FAILED",
                        "Downstream Chunker job failed",
                        downstream=downstream,
                    )
                    return

                if ds_status == "completed":
                    break

                await asyncio.sleep(POLL_INTERVAL_SEC)

            if last_stage:
                await self.finish_stage(job, last_stage)

            await self.set_stage(job, "artifact_download", message="Downloading Chunker artifacts", local_progress=0)
            artifacts_dir = self.job_dir(job.job_id) / "artifacts"
            artifacts_dir.mkdir(parents=True, exist_ok=True)

            chunks_ok = await self.download_optional(client, downstream_job_id, "chunks.jsonl", artifacts_dir / "chunks.jsonl")
            if not chunks_ok:
                await self.fail(job, "CHUNKER_ARTIFACT_MISSING", "Chunker completed but chunks.jsonl was not available")
                return
            await self.set_stage(job, "artifact_download", message="Downloading Chunker artifacts", local_progress=50)
            await self.download_optional(client, downstream_job_id, "ocr.json", artifacts_dir / "ocr.json")
            await self.set_stage(job, "artifact_download", message="Downloading Chunker artifacts", local_progress=80)
            await self.download_optional(client, downstream_job_id, "chunker.log", artifacts_dir / "chunker.log")
            await self.finish_stage(job, "artifact_download")

        chunks_path = artifacts_dir / "chunks.jsonl"
        chunk_count = 0
        if chunks_path.exists():
            with chunks_path.open("r", encoding="utf-8") as fh:
                chunk_count = sum(1 for line in fh if line.strip())

        job.status = "completed"
        job.stage = "completed"
        job.progress = 100.0
        job.message = f"Chunking completed successfully ({chunk_count} chunks)"
        job.completed_at = utcnow()
        job.eta_sec = 0.0
        job.result = {
            "chunk_count": chunk_count,
            "chunks_jsonl_download_url": f"/v1/jobs/{job.job_id}/download/chunks.jsonl",
            "ocr_download_url": f"/v1/jobs/{job.job_id}/download/ocr.json",
            "chunker_log_download_url": f"/v1/jobs/{job.job_id}/download/chunker.log",
        }
        await self.persist(job)
        await self.emit(job, "completed")

    async def download_optional(self, client: httpx.AsyncClient, downstream_job_id: str, name: str, dest: Path) -> bool:
        response = await client.get(f"{CHUNKER_BASE_URL}/v1/jobs/{downstream_job_id}/download/{name}")
        if response.status_code == 200:
            dest.write_bytes(response.content)
            return True
        return False

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
        )
        for key in [
            "created_at", "updated_at", "started_at", "completed_at", "status", "stage",
            "progress", "message", "eta_sec", "error", "result", "downstream_job_id",
            "timings", "metrics",
        ]:
            if key in data:
                setattr(job, key, data[key])
        self.jobs[job_id] = job
        return job

    async def subscribe(self, job_id: str) -> AsyncIterator[dict[str, Any]]:
        self.get(job_id)
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=100)
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


app = FastAPI(title="Book2SFT", version="0.1.0", lifespan=lifespan)

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "book2sft",
        "job_workers": JOB_WORKERS,
        "chunker_base_url": CHUNKER_BASE_URL,
    }


@app.get("/ready")
async def ready() -> JSONResponse:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{CHUNKER_BASE_URL}/health")
        ok = r.status_code == 200
        payload = {"status": "ready" if ok else "not_ready", "chunker_http_status": r.status_code}
        return JSONResponse(payload, status_code=200 if ok else 503)
    except Exception as exc:
        return JSONResponse({"status": "not_ready", "error": str(exc)}, status_code=503)


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse((FRONTEND_DIR / "index.html").read_text(encoding="utf-8"))


@app.post("/v1/jobs", status_code=202)
async def create_job(pdf_file: UploadFile = File(...)) -> dict[str, Any]:
    name = (pdf_file.filename or "").lower()
    if not name.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are accepted")
    job = await manager.create(pdf_file)
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


@app.get("/v1/jobs/{job_id}/download/{artifact}")
async def download(job_id: str, artifact: str) -> FileResponse:
    allowed = {
        "chunks.jsonl": "chunks.jsonl",
        "ocr.json": "ocr.json",
        "chunker.log": "chunker.log",
    }
    if artifact not in allowed:
        raise HTTPException(status_code=404, detail="Artifact not found")
    manager.get(job_id)
    path = manager.job_dir(job_id) / "artifacts" / allowed[artifact]
    if not path.exists():
        raise HTTPException(status_code=404, detail="Artifact not available yet")
    media = "application/jsonl" if artifact.endswith(".jsonl") else "application/json" if artifact.endswith(".json") else "text/plain"
    return FileResponse(path, filename=artifact, media_type=media)
