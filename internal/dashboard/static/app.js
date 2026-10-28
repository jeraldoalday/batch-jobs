(() => {
  "use strict";

  const STATS_ORDER = [
    "pending",
    "running",
    "retrying",
    "succeeded",
    "failed",
    "dead",
    "cancelled",
  ];
  const RETRYABLE = new Set(["dead", "failed"]);
  const CANCELABLE = new Set(["pending", "retrying"]);
  const JOBS_POLL_INTERVAL_MS = 5000;
  const SCHEDULES_POLL_INTERVAL_MS = 25000;

  const statsEl = document.getElementById("stats");
  const connBadge = document.getElementById("conn-badge");
  const statusFilter = document.getElementById("status-filter");
  const limitInput = document.getElementById("limit-input");
  const refreshBtn = document.getElementById("refresh-btn");
  const jobsError = document.getElementById("jobs-error");
  const jobsBody = document.getElementById("jobs-body");
  const schedulesPanel = document.getElementById("schedules-panel");
  const schedulesBody = document.getElementById("schedules-body");
  const schedulesError = document.getElementById("schedules-error");
  const scheduleForm = document.getElementById("schedule-form");
  const scheduleQueue = document.getElementById("schedule-queue");
  const scheduleCron = document.getElementById("schedule-cron");
  const schedulePayload = document.getElementById("schedule-payload");

  function renderStats(counts) {
    statsEl.replaceChildren(
      ...STATS_ORDER.map((status) => {
        const tile = document.createElement("div");
        tile.className = "stat-tile";

        const label = document.createElement("span");
        label.className = "label";
        label.textContent = status;

        const value = document.createElement("span");
        value.className = "value";
        value.textContent = String(counts[status] || 0);

        tile.append(label, value);
        return tile;
      })
    );
  }

  function setConn(state) {
    connBadge.classList.remove("badge-live", "badge-offline", "badge-muted");
    if (state === "live") {
      connBadge.textContent = "live";
      connBadge.classList.add("badge-live");
    } else if (state === "polling") {
      connBadge.textContent = "polling";
      connBadge.classList.add("badge-muted");
    } else {
      connBadge.textContent = "offline";
      connBadge.classList.add("badge-offline");
    }
  }

  async function fetchStats() {
    const res = await fetch("/v1/stats");
    if (!res.ok) throw new Error(`stats: ${res.status}`);
    const body = await res.json();
    renderStats(body.counts || {});
  }

  function statusPill(status) {
    const pill = document.createElement("span");
    pill.className = `status-pill status-${status}`;
    pill.textContent = status;
    return pill;
  }

  function actionButton(label, action, id) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = label;
    btn.dataset.action = action;
    btn.dataset.id = id;
    return btn;
  }

  function formatTime(iso) {
    const d = new Date(iso);
    return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
  }

  // idMeta builds the small secondary line under a job's ID showing its
  // timeout and/or idempotency key, when set, or null if neither applies.
  function idMeta(job) {
    const parts = [];
    if (job.timeout_seconds) parts.push(`timeout ${job.timeout_seconds}s`);
    if (job.idempotency_key) parts.push(`key: ${job.idempotency_key}`);
    if (parts.length === 0) return null;

    const meta = document.createElement("div");
    meta.className = "id-meta";
    meta.textContent = parts.join(" · ");
    return meta;
  }

  function renderJobs(jobs) {
    if (!jobs || jobs.length === 0) {
      const row = document.createElement("tr");
      row.className = "empty-row";
      const cell = document.createElement("td");
      cell.colSpan = 8;
      cell.textContent = "No jobs in this status.";
      row.append(cell);
      jobsBody.replaceChildren(row);
      return;
    }

    jobsBody.replaceChildren(
      ...jobs.map((job) => {
        const row = document.createElement("tr");

        const statusCell = document.createElement("td");
        statusCell.append(statusPill(job.status));

        const idCell = document.createElement("td");
        idCell.className = "id-cell";
        idCell.title = job.id;
        idCell.append(document.createTextNode(job.id));
        const meta = idMeta(job);
        if (meta) idCell.append(meta);

        const queueCell = document.createElement("td");
        queueCell.textContent = job.queue;

        const priorityCell = document.createElement("td");
        priorityCell.textContent = String(job.priority);

        const retriesCell = document.createElement("td");
        retriesCell.textContent = `${job.retry_count}/${job.max_retries}`;

        const updatedCell = document.createElement("td");
        updatedCell.textContent = formatTime(job.updated_at);

        const errorCell = document.createElement("td");
        errorCell.className = "error-cell";
        errorCell.title = job.last_error || "";
        errorCell.textContent = job.last_error || "—";

        const actionsCell = document.createElement("td");
        const actions = document.createElement("div");
        actions.className = "row-actions";
        if (CANCELABLE.has(job.status)) {
          actions.append(actionButton("Cancel", "cancel", job.id));
        }
        if (RETRYABLE.has(job.status)) {
          actions.append(actionButton("Retry", "retry", job.id));
        }
        actionsCell.append(actions);

        row.append(
          statusCell,
          idCell,
          queueCell,
          priorityCell,
          retriesCell,
          updatedCell,
          errorCell,
          actionsCell
        );
        return row;
      })
    );
  }

  async function fetchJobs() {
    const status = statusFilter.value;
    const limit = Math.max(1, Number(limitInput.value) || 50);
    const res = await fetch(`/v1/jobs?status=${encodeURIComponent(status)}&limit=${limit}`);
    if (!res.ok) throw new Error(`jobs: ${res.status}`);
    const body = await res.json();
    renderJobs(body.jobs || []);
  }

  async function refreshJobs() {
    try {
      jobsError.textContent = "";
      await fetchJobs();
    } catch (err) {
      jobsError.textContent = err.message;
    }
  }

  async function performAction(action, id) {
    const res = await fetch(`/v1/jobs/${encodeURIComponent(id)}/${action}`, { method: "POST" });
    if (!res.ok && res.status !== 204) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body.error || `${action}: ${res.status}`);
    }
    await Promise.all([refreshJobs(), fetchStats().catch(() => {})]);
  }

  jobsBody.addEventListener("click", (event) => {
    const btn = event.target.closest("button[data-action]");
    if (!btn) return;
    btn.disabled = true;
    performAction(btn.dataset.action, btn.dataset.id)
      .catch((err) => {
        jobsError.textContent = err.message;
      })
      .finally(() => {
        btn.disabled = false;
      });
  });

  refreshBtn.addEventListener("click", refreshJobs);
  statusFilter.addEventListener("change", refreshJobs);
  limitInput.addEventListener("change", refreshJobs);

  // ---- schedules -------------------------------------------------------
  //
  // The panel stays hidden until the server proves it schedules. A server
  // built without it answers 501 on every schedule route, and showing a
  // panel whose every button returns "not implemented" would be worse
  // than showing nothing.

  function stateCell(schedule) {
    const cell = document.createElement("td");
    const label = document.createElement("span");
    label.className = schedule.enabled ? "state-enabled" : "state-paused";
    label.textContent = schedule.enabled ? "enabled" : "paused";
    cell.append(label);
    return cell;
  }

  // nextRunCell shows when a schedule fires next, with the fire after that
  // beneath it, and flags an enabled schedule whose time has passed --
  // which means the scheduler is not running, or is not keeping up.
  function nextRunCell(schedule) {
    const cell = document.createElement("td");
    cell.className = "time-cell";

    const next = document.createElement("div");
    next.textContent = formatTime(schedule.next_run_at);
    if (schedule.enabled && new Date(schedule.next_run_at) < new Date()) {
      next.className = "time-overdue";
      next.title = "overdue: the scheduler has not fired this yet";
    }
    cell.append(next);

    const upcoming = schedule.upcoming || [];
    if (upcoming.length > 1) {
      const then = document.createElement("div");
      then.className = "time-meta";
      then.textContent = `then ${formatTime(upcoming[1])}`;
      cell.append(then);
    }
    return cell;
  }

  function renderSchedules(schedules) {
    if (!schedules || schedules.length === 0) {
      const row = document.createElement("tr");
      row.className = "empty-row";
      const cell = document.createElement("td");
      cell.colSpan = 6;
      cell.textContent = "No schedules yet.";
      row.append(cell);
      schedulesBody.replaceChildren(row);
      return;
    }

    schedulesBody.replaceChildren(
      ...schedules.map((schedule) => {
        const row = document.createElement("tr");

        const queueCell = document.createElement("td");
        queueCell.textContent = schedule.queue;

        const cronCell = document.createElement("td");
        cronCell.className = "cron-cell";
        cronCell.textContent = schedule.cron;

        const lastRunCell = document.createElement("td");
        lastRunCell.className = "time-cell";
        lastRunCell.textContent = schedule.last_run_at
          ? formatTime(schedule.last_run_at)
          : "never";

        const actionsCell = document.createElement("td");
        const actions = document.createElement("div");
        actions.className = "row-actions";
        actions.append(
          actionButton("Run now", "trigger", schedule.id),
          actionButton(schedule.enabled ? "Pause" : "Resume",
            schedule.enabled ? "pause" : "resume", schedule.id),
          actionButton("Delete", "delete", schedule.id)
        );
        actionsCell.append(actions);

        row.append(
          stateCell(schedule),
          queueCell,
          cronCell,
          nextRunCell(schedule),
          lastRunCell,
          actionsCell
        );
        return row;
      })
    );
  }

  async function fetchSchedules() {
    const res = await fetch("/v1/schedules");
    if (res.status === 501) return false;
    if (!res.ok) throw new Error(`schedules: ${res.status}`);
    const body = await res.json();
    renderSchedules(body.schedules || []);
    return true;
  }

  async function refreshSchedules() {
    try {
      schedulesError.textContent = "";
      if (await fetchSchedules()) schedulesPanel.hidden = false;
    } catch (err) {
      schedulesError.textContent = err.message;
    }
  }

  async function scheduleAction(action, id) {
    const path = action === "delete"
      ? `/v1/schedules/${encodeURIComponent(id)}`
      : `/v1/schedules/${encodeURIComponent(id)}/${action}`;
    const method = action === "delete" ? "DELETE" : "POST";

    const res = await fetch(path, { method });
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body.error || `${action}: ${res.status}`);
    }
    await refreshSchedules();
    // Running a schedule now puts a job in the queue, so the job views
    // are stale the moment it returns.
    if (action === "trigger") {
      await Promise.all([refreshJobs(), fetchStats().catch(() => {})]);
    }
  }

  async function createSchedule() {
    const payload = schedulePayload.value;
    const body = {
      queue: scheduleQueue.value.trim(),
      cron: scheduleCron.value.trim(),
    };
    // The API takes the payload base64-encoded, as encoding/json does for
    // any []byte; btoa is the browser's side of that.
    if (payload) body.payload = btoa(payload);

    const res = await fetch("/v1/schedules", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const failure = await res.json().catch(() => ({}));
      throw new Error(failure.error || `create schedule: ${res.status}`);
    }
    scheduleCron.value = "";
    schedulePayload.value = "";
    await refreshSchedules();
  }

  schedulesBody.addEventListener("click", (event) => {
    const btn = event.target.closest("button[data-action]");
    if (!btn) return;
    btn.disabled = true;
    scheduleAction(btn.dataset.action, btn.dataset.id)
      .catch((err) => {
        schedulesError.textContent = err.message;
      })
      .finally(() => {
        btn.disabled = false;
      });
  });

  scheduleForm.addEventListener("submit", (event) => {
    event.preventDefault();
    schedulesError.textContent = "";
    createSchedule().catch((err) => {
      schedulesError.textContent = err.message;
    });
  });

  // SSE_RECONNECT_INTERVAL_MS is how often the polling fallback retries a
  // live connection. A dropped EventSource (a deploy, a network blip) would
  // otherwise downgrade the dashboard to polling for the rest of the page's
  // life, with no way back to "live" short of a reload.
  const SSE_RECONNECT_INTERVAL_MS = 30000;

  let pollTimer = null;
  let reconnectTimer = null;

  function startPollingFallback() {
    setConn("polling");
    if (!pollTimer) {
      pollTimer = setInterval(() => {
        fetchStats().catch(() => setConn("offline"));
      }, JOBS_POLL_INTERVAL_MS);
    }
    if (!reconnectTimer) {
      reconnectTimer = setInterval(connectEvents, SSE_RECONNECT_INTERVAL_MS);
    }
  }

  // stopPollingFallback cancels the polling fallback once a live connection
  // is back, so the dashboard isn't still polling and retrying underneath
  // a working EventSource.
  function stopPollingFallback() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
    if (reconnectTimer) {
      clearInterval(reconnectTimer);
      reconnectTimer = null;
    }
  }

  function connectEvents() {
    if (typeof EventSource === "undefined") {
      startPollingFallback();
      return;
    }
    const source = new EventSource("/v1/events");
    source.onopen = () => {
      setConn("live");
      stopPollingFallback();
    };
    source.onmessage = (event) => {
      try {
        const body = JSON.parse(event.data);
        renderStats(body.counts || {});
      } catch {
        // Ignore malformed frames; the next one will correct the view.
      }
    };
    source.onerror = () => {
      setConn("offline");
      source.close();
      startPollingFallback();
    };
  }

  fetchStats().catch((err) => {
    jobsError.textContent = err.message;
  });
  refreshJobs();
  refreshSchedules();
  connectEvents();
  setInterval(refreshJobs, JOBS_POLL_INTERVAL_MS);
  // Schedules change far less often than jobs, and a firing only moves a
  // timestamp, so they are re-read at a fifth of the job cadence.
  setInterval(refreshSchedules, SCHEDULES_POLL_INTERVAL_MS);
})();
