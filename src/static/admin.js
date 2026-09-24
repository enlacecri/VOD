const state = { key: (typeof sessionStorage !== "undefined" ? sessionStorage.getItem("vodAdminKey") : "") || "", timer: null };
const $ = (id) => (typeof document !== "undefined" ? document.getElementById(id) : null);

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { "X-Admin-Key": state.key, ...(options.headers || {}) },
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.detail?.message || body.detail || `Error HTTP ${response.status}`);
  return body;
}

function setText(id, value) { $(id).textContent = String(value); }
function showToast(message) {
  const toast = $("toast");
  toast.textContent = message;
  toast.classList.add("visible");
  window.setTimeout(() => toast.classList.remove("visible"), 3200);
}

function renderBars(counts) {
  const container = $("statusBars");
  container.replaceChildren();
  const order = ["ready", "processing", "validating", "queued", "probing", "created", "failed"];
  const max = Math.max(1, ...Object.values(counts));
  order.filter((name) => counts[name]).forEach((name) => {
    const row = document.createElement("div");
    row.className = `bar-row ${name}`;
    const label = document.createElement("span"); label.className = "bar-label"; label.textContent = name;
    const track = document.createElement("div"); track.className = "bar-track";
    const fill = document.createElement("div"); fill.className = "bar-fill"; fill.style.width = `${(counts[name] / max) * 100}%`;
    const count = document.createElement("span"); count.className = "bar-count"; count.textContent = counts[name];
    track.append(fill); row.append(label, track, count); container.append(row);
  });
}

function actionButton(label, endpoint) {
  const button = document.createElement("button");
  button.type = "button"; button.className = "button ghost"; button.textContent = label;
  button.addEventListener("click", async () => {
    button.disabled = true;
    try {
      await api(endpoint, { method: "POST" });
      showToast(`${label}: solicitud aceptada`);
      await loadDashboard();
    } catch (error) { showToast(error.message); }
    finally { button.disabled = false; }
  });
  return button;
}

function renderFailures(failures) {
  const list = $("failureList"); list.replaceChildren();
  $("emptyFailures").classList.toggle("hidden", failures.length !== 0);
  setText("failureTotal", `${failures.length} incidente${failures.length === 1 ? "" : "s"}`);
  failures.forEach((failure) => {
    const row = document.createElement("div"); row.className = "failure-row";
    const title = document.createElement("div"); title.className = "failure-title";
    const name = document.createElement("strong"); name.textContent = failure.enlace_id;
    const uuid = document.createElement("span"); uuid.textContent = failure.vod_uuid;
    title.append(name, uuid);
    const code = document.createElement("span"); code.className = "error-code"; code.textContent = failure.error_code || "SIN_CÓDIGO";
    const message = document.createElement("span"); message.className = "failure-message"; message.textContent = failure.error_message || "Sin detalle disponible";
    const actions = document.createElement("div"); actions.className = "action-group";
    actions.append(
      actionButton("Reintentar probe", `/api/v1/assets/${failure.vod_uuid}/retry`),
      actionButton("Reintentar HLS", `/api/v1/assets/${failure.vod_uuid}/retry-transcode`),
    );
    row.append(title, code, message, actions); list.append(row);
  });
}

function render(data) {
  const assets = data.assets || {};
  setText("readyCount", assets.ready || 0);
  setText("processingCount", (assets.created || 0) + (assets.probing || 0) + (assets.validating || 0) + (assets.queued || 0) + (assets.processing || 0));
  setText("failedCount", assets.failed || 0);
  setText("queueDepth", data.queue.depth || 0);
  setText("workerCount", data.queue.workers || 0);
  setText("staleCount", data.queue.stale_jobs || 0);
  const warning = data.queue.stale_jobs > 0 || (data.queue.depth > 0 && data.queue.workers === 0);
  const system = $("systemState"); system.className = `system-state ${warning ? "warning" : "healthy"}`;
  system.lastElementChild.textContent = warning ? "Requiere atención" : "Sistema operativo";
  setText("capacityMessage", data.queue.workers === 0 && data.queue.depth > 0
    ? "Hay trabajos esperando y ningún worker disponible."
    : `${data.queue.workers} worker(s) atendiendo una cola de ${data.queue.depth} trabajo(s).`);
  setText("updatedAt", `Actualizado ${new Date(data.generated_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`);
  renderBars(assets); renderFailures(data.recent_failures || []);
}

async function loadDashboard() {
  if (!state.key) return;
  $("refreshButton").disabled = true;
  let delay = 30000;
  try {
    const data = await api("/api/v1/admin/dashboard");
    render(data);
    $("loginPanel").classList.add("hidden");
    $("dashboard").classList.remove("hidden");
    $("logoutButton").classList.remove("hidden");
    $("loginError").textContent = "";
    
    // Fetch active processing assets
    const processingStates = ["created", "probing", "validating", "queued", "processing"];
    const activeAssetsChunks = await Promise.all(
      processingStates.map(status => api(`/api/v1/admin/assets?status=${status}&limit=20`))
    );
    const activeAssets = activeAssetsChunks.flatMap(resp => resp.items);
    renderAssetList(activeAssets, "processingList", "emptyProcessing", "processingTotal", true);
    
    // Refresh if ready panel is visible
    if (!$("readyPanel").classList.contains("hidden")) {
      fetchAndRenderAssets("ready", "readyList", "emptyReady", "readyTotal");
    }
    
    if (activeAssets.length > 0) delay = 5000;
  } catch (error) {
    $("loginError").textContent = error.message;
    if (/credentials|configurada|configured/i.test(error.message)) {
      logout(false);
      return;
    }
  } finally {
    $("refreshButton").disabled = false;
    if (state.key) scheduleNext(delay);
  }
}

function scheduleNext(delay) {
  if (state.timer) clearTimeout(state.timer);
  state.timer = setTimeout(loadDashboard, delay);
}

function logout(showMessage = true) {
  if (state.timer) clearTimeout(state.timer);
  state.key = ""; sessionStorage.removeItem("vodAdminKey");
  $("dashboard").classList.add("hidden"); $("loginPanel").classList.remove("hidden");
  $("logoutButton").classList.add("hidden");
  const system = $("systemState"); system.className = "system-state neutral"; system.lastElementChild.textContent = "Esperando conexión";
  if (showMessage) showToast("Sesión cerrada");
}

$("loginForm").addEventListener("submit", async (event) => {
  event.preventDefault(); state.key = $("adminKey").value; sessionStorage.setItem("vodAdminKey", state.key); await loadDashboard();
});
$("refreshButton").addEventListener("click", loadDashboard);
$("logoutButton").addEventListener("click", () => logout());

if (state.key) loadDashboard();

$("viewReadyBtn").addEventListener("click", () => {
  const panel = $("readyPanel");
  panel.classList.toggle("hidden");
  if (!panel.classList.contains("hidden")) {
    fetchAndRenderAssets("ready", "readyList", "emptyReady", "readyTotal");
    setTimeout(() => panel.scrollIntoView({ behavior: "smooth", block: "start" }), 100);
  }
});

$("closeModalBtn")?.addEventListener("click", () => {
  $("detailsModal").classList.add("hidden");
});

async function fetchAndRenderAssets(status, listId, emptyId, totalId) {
  try {
    const data = await api(`/api/v1/admin/assets?status=${status}&limit=50`);
    renderAssetList(data.items, listId, emptyId, totalId, status !== "ready");
  } catch (error) {
    showToast(error.message);
  }
}

let activeTicker = null;

function formatDuration(seconds) {
  if (seconds == null || seconds === "" || isNaN(seconds) || seconds < 0) return "—";
  const total = Math.round(Number(seconds));
  if (total === 0) return "0s";
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  const parts = [];
  if (h > 0) parts.push(`${h}h`);
  if (m > 0) parts.push(`${m}m`);
  if (s > 0 || parts.length === 0) parts.push(`${s}s`);
  return parts.join(" ");
}

function formatRatio(ratio) {
  if (ratio == null || ratio === "" || isNaN(ratio)) return "—";
  return `${Number(ratio).toFixed(2)}x`;
}

function startActiveTimer() {
  if (activeTicker) clearInterval(activeTicker);
  activeTicker = setInterval(() => {
    const timerNodes = document.querySelectorAll("[data-started-at]");
    if (timerNodes.length === 0) {
      clearInterval(activeTicker);
      activeTicker = null;
      return;
    }
    timerNodes.forEach((el) => {
      const started = new Date(el.dataset.startedAt).getTime();
      if (!isNaN(started)) {
        const sec = Math.max(0, Math.floor((Date.now() - started) / 1000));
        el.textContent = `Procesando: ${formatDuration(sec)}`;
      }
    });
  }, 1000);
}

function showDetails(asset) {
  $("modalTitle").textContent = "Detalles: " + asset.enlace_id;
  const body = $("modalBody");
  body.replaceChildren();
  
  const addRow = (label, value) => {
    const p = document.createElement("p");
    const strong = document.createElement("strong");
    strong.textContent = label + ": ";
    const span = document.createElement("span");
    span.textContent = value || "N/D";
    p.append(strong, span);
    body.append(p);
  };
  
  const addDivider = (title) => {
    const hr = document.createElement("hr");
    hr.style.margin = "16px 0 8px 0";
    hr.style.border = "none";
    hr.style.borderTop = "1px solid var(--line)";
    body.append(hr);
    
    if (title) {
        const h3 = document.createElement("h3");
        h3.textContent = title;
        h3.style.margin = "0 0 8px 0";
        h3.style.fontSize = "1em";
        h3.style.color = "var(--ink)";
        body.append(h3);
    }
  };
  
  addRow("UUID", asset.vod_uuid);
  addRow("Estado", asset.status);
  addRow("Duración del video", formatDuration(asset.duration_seconds));
  addRow("Resolución", (asset.source_width && asset.source_height) ? `${asset.source_width}x${asset.source_height}` : "—");
  addRow("Video Codec", asset.video_codec);
  addRow("Audio Codec", asset.audio_codec);
  addRow("Publicado", asset.published_at ? new Date(asset.published_at).toLocaleString() : "N/D");
  addRow("Ruta Staging", asset.staged_source_path);
  addRow("Ubicación archivada", asset.processed_source_path || "N/D");
  addRow("Ruta Manifest", asset.manifest_path);
  addRow("URL Playback", asset.playback_url);
  
  addDivider("Métricas de Procesamiento");
  addRow("Tiempo de procesamiento", asset.processing_time_seconds != null ? formatDuration(asset.processing_time_seconds) : "—");
  addRow("Ratio de procesamiento", formatRatio(asset.processing_ratio));
  addRow("Inicio de procesamiento", asset.processing_started_at ? new Date(asset.processing_started_at).toLocaleString() : "N/D");
  addRow("Fin de procesamiento", asset.processing_finished_at ? new Date(asset.processing_finished_at).toLocaleString() : "N/D");
  addRow("Tiempo esperando en cola", asset.queue_time_seconds != null ? formatDuration(asset.queue_time_seconds) : (asset.queue_wait_seconds != null ? formatDuration(asset.queue_wait_seconds) : "—"));
  addRow("Duración de análisis (probe)", formatDuration(asset.probe_processing_seconds));
  addRow("Duración de transcodificación", formatDuration(asset.transcode_processing_seconds));
  
  if (asset.elapsed_wall_seconds != null) {
      addRow("Tiempo total transcurrido", formatDuration(asset.elapsed_wall_seconds));
  } else if (asset.created_at && asset.published_at) {
      const estimated = (new Date(asset.published_at) - new Date(asset.created_at)) / 1000;
      addRow("Tiempo total transcurrido estimado", formatDuration(estimated));
  } else {
      addRow("Tiempo total transcurrido", "N/D");
  }
  addDivider();
  
  if (asset.variants && asset.variants.length > 0) {
    const vp = document.createElement("p");
    const vstrong = document.createElement("strong");
    vstrong.textContent = "Variantes HLS: ";
    vp.append(vstrong);
    const ul = document.createElement("ul");
    asset.variants.forEach(v => {
      const li = document.createElement("li");
      li.textContent = `${v.name} - ${v.width}x${v.height} (V: ${v.video_bitrate}, A: ${v.audio_bitrate})`;
      ul.append(li);
    });
    vp.append(ul);
    body.append(vp);
  }

  if (asset.playback_url) {
    const playP = document.createElement("p");
    playP.style.marginTop = "16px";
    const playBtn = document.createElement("button");
    playBtn.className = "button primary";
    playBtn.textContent = "▶ Abrir en Reproductor Web";
    playBtn.addEventListener("click", () => window.open(`/experimental/asset-player?vod_uuid=${asset.vod_uuid}`, "_blank"));
    playP.append(playBtn);
    body.append(playP);
  }
  
  $("detailsModal").classList.remove("hidden");
}

function renderAssetList(assets, listId, emptyId, totalId, showProgress) {
  const list = $(listId); list.replaceChildren();
  $(emptyId).classList.toggle("hidden", assets.length !== 0);
  setText(totalId, `${assets.length} video${assets.length === 1 ? "" : "s"}`);
  assets.forEach((asset) => {
    const row = document.createElement("div"); row.className = "asset-row";
    const title = document.createElement("div"); title.className = "failure-title";
    const name = document.createElement("strong"); name.textContent = asset.enlace_id;
    const uuid = document.createElement("span"); uuid.textContent = asset.vod_uuid;
    title.append(name, uuid);
    
    const info = document.createElement("div"); info.className = "asset-info";
    if (showProgress) {
      if (asset.status === "failed") {
        const statusText = document.createElement("span");
        statusText.style.color = "var(--red)";
        statusText.style.fontWeight = "700";
        statusText.textContent = "Falló";

        const timeText = document.createElement("span");
        timeText.textContent = asset.processing_time_seconds != null 
          ? `Falló después de ${formatDuration(asset.processing_time_seconds)}` 
          : "—";

        const ratioSpan = document.createElement("span");
        ratioSpan.className = "ratio-badge";
        ratioSpan.textContent = "—";

        const date = document.createElement("span");
        date.textContent = asset.updated_at ? new Date(asset.updated_at).toLocaleDateString() : (asset.created_at ? new Date(asset.created_at).toLocaleDateString() : "—");

        info.append(statusText, timeText, ratioSpan, date);
      } else {
        const statusText = document.createElement("span");
        statusText.textContent = asset.progress ? `Procesando · ${asset.progress}%` : `Estado: ${asset.status}`;
        statusText.style.fontWeight = "600";
        statusText.style.color = "var(--ink)";

        if (asset.progress != null && asset.progress > 0) {
          const progTrack = document.createElement("div"); progTrack.className = "bar-track"; progTrack.style.width = "80px"; progTrack.style.display = "inline-block";
          const progFill = document.createElement("div"); progFill.className = "bar-fill"; progFill.style.width = `${asset.progress}%`;
          progTrack.append(progFill);
          info.append(statusText, progTrack);
        } else {
          info.append(statusText);
        }

        const timerSpan = document.createElement("span");
        if (asset.processing_started_at) {
          const elapsed = Math.max(0, Math.floor((Date.now() - new Date(asset.processing_started_at).getTime()) / 1000));
          timerSpan.textContent = `Procesando: ${formatDuration(elapsed)}`;
          timerSpan.setAttribute("data-started-at", asset.processing_started_at);
        } else {
          timerSpan.textContent = "En cola";
        }

        const ratioSpan = document.createElement("span");
        ratioSpan.className = "ratio-badge";
        ratioSpan.textContent = "—";
        ratioSpan.title = "Ratio no disponible mientras el video esté en procesamiento";

        const date = document.createElement("span");
        date.textContent = asset.created_at ? new Date(asset.created_at).toLocaleDateString() : "—";

        info.append(timerSpan, ratioSpan, date);
      }
    } else {
      const duration = document.createElement("span");
      duration.textContent = formatDuration(asset.duration_seconds);

      const res = document.createElement("span");
      res.textContent = (asset.source_width && asset.source_height) ? `${asset.source_width}x${asset.source_height}` : "—";

      const proc = document.createElement("span");
      proc.textContent = asset.processing_time_seconds != null
        ? `Procesó en ${formatDuration(asset.processing_time_seconds)}`
        : "—";

      const ratioSpan = document.createElement("span");
      ratioSpan.className = "ratio-badge";
      ratioSpan.textContent = formatRatio(asset.processing_ratio);
      if (asset.processing_ratio != null) {
        const pct = Math.round(asset.processing_ratio * 100);
        ratioSpan.title = `Tiempo de procesamiento ÷ duración del video (${pct}% de la duración)`;
      } else {
        ratioSpan.title = "Ratio no disponible";
      }

      const date = document.createElement("span");
      date.textContent = asset.published_at ? new Date(asset.published_at).toLocaleDateString() : (asset.created_at ? new Date(asset.created_at).toLocaleDateString() : "—");

      info.append(duration, res, proc, ratioSpan, date);
    }
    
    const actions = document.createElement("div"); actions.className = "action-group";
    
    if (asset.playback_url) {
      const btnOpen = document.createElement("button"); btnOpen.className = "button primary"; btnOpen.textContent = "▶ Reproducir";
      btnOpen.addEventListener("click", () => window.open(`/experimental/asset-player?vod_uuid=${asset.vod_uuid}`, "_blank"));
      
      const btnCopy = document.createElement("button"); btnCopy.className = "button ghost"; btnCopy.textContent = "Copiar HLS";
      btnCopy.addEventListener("click", async () => {
        try {
          await navigator.clipboard.writeText(asset.playback_url);
          showToast("URL HLS copiada al portapapeles");
        } catch (e) {
          showToast("Error copiando URL");
        }
      });
      actions.append(btnOpen, btnCopy);
    }
    
    const btnDetails = document.createElement("button"); btnDetails.className = "button ghost"; btnDetails.textContent = "Ver detalles";
    btnDetails.addEventListener("click", () => showDetails(asset));
    actions.append(btnDetails);
    
    row.append(title, info, actions);
    list.append(row);
  });

  startActiveTimer();
}

if (typeof module !== "undefined" && module.exports) {
  module.exports = {
    formatDuration,
    formatRatio,
    renderAssetList,
    showDetails,
  };
}
