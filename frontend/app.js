const $ = (id) => document.getElementById(id);
let source = null;
let currentJob = null;

function seconds(v) {
  if (v === null || v === undefined) return "—";
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  if (n < 60) return `${n.toFixed(1)}s`;
  const m = Math.floor(n / 60);
  const s = Math.round(n % 60);
  return `${m}m ${s}s`;
}

function elapsedFrom(iso) {
  if (!iso) return 0;
  return Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
}

function render(job) {
  currentJob = job;
  $("jobPanel").classList.remove("hidden");
  $("jobTitle").textContent = job.filename || job.job_id;
  $("jobMessage").textContent = job.message || "—";
  $("jobStatus").textContent = job.status;
  $("stageName").textContent = job.stage;
  const pct = Math.max(0, Math.min(100, Number(job.progress || 0)));
  $("overallPct").textContent = `${pct.toFixed(1)}%`;
  $("overallBar").style.width = `${pct}%`;
  $("elapsed").textContent = seconds(job.total_elapsed_sec);
  $("eta").textContent = seconds(job.eta_sec);

  const timings = job.timings || {};
  $("timings").innerHTML = Object.entries(timings).map(([name, t]) => `
    <div class="timing-row">
      <strong>${name}</strong>
      <span class="state">${t.status || "waiting"}</span>
      <span>${seconds(t.elapsed_sec || 0)}</span>
    </div>
  `).join("") || '<p class="muted">هنوز timing ثبت نشده است.</p>';

  if (job.status === "completed" && job.result) {
    $("downloads").classList.remove("hidden");
    $("chunksLink").href = job.result.chunks_jsonl_download_url;
    $("ocrLink").href = job.result.ocr_download_url;
    $("logLink").href = job.result.chunker_log_download_url;
  }

  if (job.status === "failed") {
    $("errorBox").classList.remove("hidden");
    $("errorBox").textContent = JSON.stringify(job.error, null, 2);
  }
}

async function checkHealth() {
  try {
    const r = await fetch('/ready');
    const data = await r.json();
    $("health").textContent = r.ok ? 'آماده' : 'Chunker در دسترس نیست';
  } catch (_) {
    $("health").textContent = 'خطای اتصال';
  }
}

$("pdfFile").addEventListener("change", (e) => {
  const f = e.target.files?.[0];
  $("fileName").textContent = f ? `${f.name} — ${(f.size / 1024 / 1024).toFixed(2)} MB` : 'هنوز فایلی انتخاب نشده';
});

$("uploadForm").addEventListener("submit", async (e) => {
  e.preventDefault();
  const file = $("pdfFile").files?.[0];
  if (!file) return;
  $("submitBtn").disabled = true;
  $("submitBtn").textContent = 'در حال ارسال…';
  $("downloads").classList.add("hidden");
  $("errorBox").classList.add("hidden");

  try {
    const form = new FormData();
    form.append('pdf_file', file);
    const r = await fetch('/v1/jobs', { method: 'POST', body: form });
    const data = await r.json();
    if (!r.ok) throw new Error(JSON.stringify(data));
    connectEvents(data.job_id);
  } catch (err) {
    alert(`Upload failed: ${err}`);
  } finally {
    $("submitBtn").disabled = false;
    $("submitBtn").textContent = 'شروع پردازش';
  }
});

function connectEvents(jobId) {
  if (source) source.close();
  source = new EventSource(`/v1/jobs/${jobId}/events`);
  source.onmessage = (e) => {
    const event = JSON.parse(e.data);
    if (event.job) render(event.job);
    if (event.type === 'completed' || event.type === 'failed') source.close();
  };
  source.onerror = async () => {
    try {
      const r = await fetch(`/v1/jobs/${jobId}`);
      if (r.ok) render(await r.json());
    } catch (_) {}
  };
}

checkHealth();
setInterval(() => {
  if (currentJob && currentJob.status === 'running') render(currentJob);
}, 1000);
