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

  const statsEl = document.getElementById("stats");
  const connBadge = document.getElementById("conn-badge");
  const statusFilter = document.getElementById("status-filter");
  const limitInput = document.getElementById("limit-input");
  const refreshBtn = document.getElementById("refresh-btn");
  const jobsError = document.getElementById("jobs-error");
  const jobsBody = document.getElementById("jobs-body");

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
        idCell.textContent = job.id;

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

  function startPollingFallback() {
    setConn("polling");
    setInterval(() => {
      fetchStats().catch(() => setConn("offline"));
    }, JOBS_POLL_INTERVAL_MS);
  }

  function connectEvents() {
    if (typeof EventSource === "undefined") {
      startPollingFallback();
      return;
    }
    const source = new EventSource("/v1/events");
    source.onopen = () => setConn("live");
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
  connectEvents();
  setInterval(refreshJobs, JOBS_POLL_INTERVAL_MS);
})();
