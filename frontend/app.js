const $ = (id) => document.getElementById(id);
let source = null;
let currentJob = null;
let activeJobId = null;
let statusPollInFlight = false;
let lastStatusFetchAt = 0;

// Time must advance locally even when the backend sends no new progress events.
function liveElapsed(startedAt, completedAt, fallback = 0) {
  if (!startedAt) return Number(fallback) || 0;
  const start = Date.parse(startedAt);
  const end = completedAt ? Date.parse(completedAt) : Date.now();
  if (!Number.isFinite(start) || !Number.isFinite(end)) return Number(fallback) || 0;
  return Math.max(0, (end - start) / 1000);
}

async function refreshJobStatus(jobId) {
  if (!jobId || statusPollInFlight) return;
  statusPollInFlight = true;
  try {
    const response = await fetch(`/v1/jobs/${encodeURIComponent(jobId)}`, { cache: 'no-store' });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const state = await response.json();
    if (activeJobId !== jobId) return;
    lastStatusFetchAt = Date.now();
    render(state);
    if (state.status === 'completed' || state.status === 'failed') {
      if (source) source.close();
    }
  } catch (error) {
    console.warn('Job status polling failed:', error);
  } finally {
    statusPollInFlight = false;
  }
}


const STAGE_LABELS = {
  upload: "آپلود PDF",
  chunker_submit: "ارسال به Chunker",
  chunker_waiting: "انتظار در صف Chunker",
  ocr: "OCR",
  normalizing: "نرمال‌سازی OCR",
  chunking: "آماده‌سازی Chunker",
  book_plan: "BookPlan",
  atomizing: "Atomizer",
  planning: "Planner",
  validating: "Chunk Validation",
  chunker_finalizing: "Chunker Finalization",
  chunker_artifacts: "دریافت خروجی Chunker",
  sft_prepare: "آماده‌سازی SFT",
  sft_generation: "SFT Data Generator",
  sft_book_selection: "Book-level SFT Selection",
  sft_finalize: "ساخت JSONLهای نهایی",
  review_pdf: "ساخت Review PDF",
};

const TIMING_LABELS = {
  "instruction_value_assessment_sec": "Instruction Value Assessment",
  "context_assembly_sec": "Context Assembly",
  "generate_instruction_sec": "Generate Instruction (wall sum)",
  "candidate_processing_wall_sec": "Candidate Processing (wall sum)",
  "pair_translate_sum_sec": "Translation sum",
  "pair_evaluate_sum_sec": "Quality Evaluation sum",
  "pair_total_sum_sec": "Pair processing sum",
  "final_selection_sec": "Per-chunk final selection",
  "total_sec": "Per-chunk total sum",
  "generate_instruction_service.input_preparation_sec": "Generator · Input Preparation",
  "generate_instruction_service.content_profile_sec": "Generator · Content Profile",
  "generate_instruction_service.task_selection_sec": "Generator · Task Selection",
  "generate_instruction_service.semantic_target_planner_sec": "Generator · Semantic Target Planner",
  "generate_instruction_service.target_task_assignment_sec": "Generator · Target→Task Assignment",
  "generate_instruction_service.diversity_sampling_sum_sec": "Generator · Diversity Sampling sum",
  "generate_instruction_service.prompt_build_sum_sec": "Generator · Prompt Build sum",
  "generate_instruction_service.llm_generation_sum_sec": "Generator · LLM Calls sum",
  "generate_instruction_service.parse_sum_sec": "Generator · Parse sum",
  "generate_instruction_service.metadata_attach_sum_sec": "Generator · Metadata Attach sum",
  "generate_instruction_service.validation_sum_sec": "Generator · Validation sum",
  "generate_instruction_service.targeted_generation_wall_sec": "Generator · Targeted Generation wall sum",
  "generate_instruction_service.semantic_dedup_sec": "Generator · Semantic Dedup",
  "telemetry.candidate_processing.timings.evaluate_sum_sec": "Evaluator · Quality Evaluation sum",
  "telemetry.candidate_processing.timings.evaluation_llm_sum_sec": "Evaluator · LLM sum",
};

function seconds(v) {
  if (v === null || v === undefined) return "—";
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  if (n < 60) return `${n.toFixed(1)}s`;
  const h = Math.floor(n / 3600);
  const m = Math.floor((n % 3600) / 60);
  const s = Math.round(n % 60);
  return h > 0 ? `${h}h ${m}m ${s}s` : `${m}m ${s}s`;
}

function timingName(raw) {
  const stripped = raw.replace(/^telemetry\.instruction_generation\.timings\./, "generate_instruction_service.")
                      .replace(/^telemetry\.instruction_value_assessment\.timings\./, "assessment_service.");
  return TIMING_LABELS[stripped] || stripped.replaceAll("_", " ");
}

function renderSft(job) {
  const sft = job.metrics?.pipeline?.sft;
  if (!sft || !sft.total_chunks) {
    $("sftPanel").classList.add("hidden");
    return;
  }
  $("sftPanel").classList.remove("hidden");
  $("sftChunks").textContent = `${sft.completed_chunks || 0} / ${sft.total_chunks || 0} chunks`;
  $("sftActive").textContent = sft.active_chunks || 0;
  const live = sft.targets_live || {};
  $("sftTargets").textContent = live.total ? `${live.completed || 0} / ${live.total}` : "—";
  $("sftGenerated").textContent = sft.generated_pairs || 0;
  $("sftQuality").textContent = sft.quality_pass_pairs || 0;
  $("sftSelected").textContent = sft.selected_pairs || 0;
  $("sftFailed").textContent = sft.failed_chunks || 0;

  const targets = Array.isArray(sft.current_targets) ? sft.current_targets : [];
  $("currentTargets").innerHTML = targets.map(t => `
    <div class="target-row">
      <div><strong>${t.chunk_id || "chunk"}</strong><span>${t.stage || "running"}${t.target_id ? ` · ${t.target_id}` : ""}${t.task_type ? ` · ${t.task_type}` : ""}</span></div>
      <strong>${Number(t.percent || 0).toFixed(1)}%</strong>
    </div>
  `).join("");

  const timingEntries = Object.entries(sft.timings_sum_sec || {})
    .filter(([, value]) => Number(value) > 0)
    .sort((a, b) => Number(b[1]) - Number(a[1]));
  $("sftTimings").innerHTML = timingEntries.map(([name, value]) => `
    <div class="timing-row">
      <strong>${timingName(name)}</strong>
      <span class="state">sum across completed chunks</span>
      <span>${seconds(value)}</span>
    </div>
  `).join("") || '<p class="muted">با تکمیل اولین chunk، timingهای داخلی اینجا ظاهر می‌شوند.</p>';
}

function renderDownloads(job) {
  const downloads = job.result?.downloads || {};
  const specs = [
    ["sft_jsonl", "دانلود SFT JSONL"],
    ["sft_with_chunks_jsonl", "SFT + Source Chunk JSONL"],
    ["sft_axolotl_jsonl", "Axolotl JSONL"],
    ["review_pdf", "Review PDF"],
    ["review_all_pdf", "Audit PDF (همه نمونه‌ها)"],
    ["all_evaluated_jsonl", "All Evaluated JSONL"],
    ["sft_metrics_json", "SFT Timing / Metrics"],
    ["sft_errors_jsonl", "SFT Errors"],
    ["chunks_jsonl", "Chunks JSONL"],
    ["ocr_json", "OCR JSON"],
    ["chunker_log", "Chunker Log"],
  ];
  const links = specs.filter(([key]) => downloads[key]).map(([key, label]) =>
    `<a class="button secondary" href="${downloads[key]}">${label}</a>`
  );
  if (links.length) {
    $("downloads").innerHTML = links.join("");
    $("downloads").classList.remove("hidden");
  }
}

function render(job) {
  currentJob = job;
  $("jobPanel").classList.remove("hidden");
  $("jobTitle").textContent = job.book_title || job.filename || job.job_id;
  $("jobMessage").textContent = job.message || "—";
  $("jobStatus").textContent = job.status;
  $("stageName").textContent = STAGE_LABELS[job.stage] || job.stage;
  const pct = Math.max(0, Math.min(100, Number(job.progress || 0)));
  const stagePct = Math.max(0, Math.min(100, Number(job.stage_progress || 0)));
  $("overallPct").textContent = `${pct.toFixed(1)}%`;
  $("overallBar").style.width = `${pct}%`;
  $("stagePct").textContent = `${stagePct.toFixed(1)}%`;
  $("elapsed").textContent = seconds(liveElapsed(job.started_at, job.completed_at, job.total_elapsed_sec));
  $("eta").textContent = seconds(job.eta_sec);

  const timings = job.timings || {};
  $("timings").innerHTML = Object.entries(timings).map(([name, t]) => `
    <div class="timing-row">
      <strong>${STAGE_LABELS[name] || name}</strong>
      <span class="state">${t.status || "waiting"}</span>
      <span>${seconds(t.status === "running" ? liveElapsed(t.started_at, t.finished_at, t.elapsed_sec) : (t.elapsed_sec || 0))}</span>
    </div>
  `).join("") || '<p class="muted">هنوز timing ثبت نشده است.</p>';

  renderSft(job);

  if (job.status === "completed" && job.result) {
    $("resultSummary").classList.remove("hidden");
    $("resultChunks").textContent = job.result.chunk_count || 0;
    $("resultAllPairs").textContent = job.result.all_evaluated_pairs || 0;
    $("resultSelected").textContent = job.result.selected_sft_pairs || 0;
    const warnings = job.result.warnings || [];
    if (warnings.length) {
      $("warnings").classList.remove("hidden");
      $("warnings").textContent = warnings.join("\n");
    }
    renderDownloads(job);
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
    if (r.ok) {
      $("health").textContent = 'همه سرویس‌ها آماده‌اند';
    } else {
      const bad = Object.entries(data.services || {}).filter(([,v]) => !v.ok).map(([k]) => k).join(', ');
      $("health").textContent = `آماده نیست: ${bad || 'service error'}`;
    }
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
  $("resultSummary").classList.add("hidden");
  $("errorBox").classList.add("hidden");
  try {
    const form = new FormData();
    form.append('pdf_file', file);
    form.append('book_title', $("bookTitle").value || '');
    form.append('category', $("category").value || '');
    form.append('domain', $("domain").value || '');
    const r = await fetch('/v1/jobs', { method: 'POST', body: form });
    const data = await r.json();
    if (!r.ok) throw new Error(JSON.stringify(data));
    connectEvents(data.job_id);
  } catch (err) {
    alert(`Upload failed: ${err}`);
  } finally {
    $("submitBtn").disabled = false;
    $("submitBtn").textContent = 'شروع کل Pipeline';
  }
});

function connectEvents(jobId) {
  activeJobId = jobId;
  if (source) source.close();
  refreshJobStatus(jobId);
  source = new EventSource(`/v1/jobs/${jobId}/events`);
  source.onmessage = (e) => {
    const event = JSON.parse(e.data);
    if (event.job) render(event.job);
    if (event.type === 'completed' || event.type === 'failed') source.close();
  };
  source.onerror = () => {
    // EventSource reconnects automatically; independent polling covers missed events.
    refreshJobStatus(jobId);
  };
}

checkHealth();
setInterval(() => {
  if (currentJob && currentJob.status === 'running') render(currentJob);
}, 1000);

// SSE can disconnect or only emit keepalive pings; never rely on it as the
// sole source of job state. A separate polling loop retrieves authoritative data.
setInterval(() => {
  if (activeJobId && (!currentJob || !['completed', 'failed'].includes(currentJob.status))) {
    refreshJobStatus(activeJobId);
  }
}, 5000);