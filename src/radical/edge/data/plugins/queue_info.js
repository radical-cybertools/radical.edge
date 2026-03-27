/**
 * Queue Info Plugin Module for Radical Edge Explorer
 *
 * Displays SLURM queue/partition info, job listings, and allocations.
 */

export const name = 'queue_info';

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">📋</div>
      <h2>Queue Info — <span class="edge-label"></span></h2>
      <button class="btn btn-secondary btn-sm" style="margin-left:auto" data-action="refresh">↺ Refresh</button>
    </div>
    <div class="qi-allocation-area"></div>
    <div class="queueinfo-content">
      <div class="empty">
        <div class="empty-icon">⏳</div>
        <p>Loading…</p>
      </div>
    </div>
  `;
}

export function css() {
  return `
    .qi-job-row {
      cursor: pointer;
      transition: background 0.15s;
    }
    .qi-job-row:hover {
      background: var(--hover);
    }
    .qi-job-row.selected {
      background: rgba(59, 130, 246, 0.15);
      border-left: 3px solid var(--primary);
    }
    .qi-alloc-card {
      margin-bottom: 8px;
    }
    .qi-alloc-active {
      border-left: 3px solid var(--primary);
    }
    .qi-alloc-grid {
      display: grid;
      grid-template-columns: 120px 1fr;
      gap: 4px 16px;
      align-items: center;
      margin-top: 6px;
    }
    .qi-alloc-label {
      color: var(--muted);
      font-size: 0.85em;
    }
    .qi-alloc-value {
      font-weight: 500;
    }
  `;
}

// Module-level state
let currentJobsData = [];
let myJobsData = [];
let queueDataCache = {};

// Shared with api.escHtml — set in init()
let escHtml = s => String(s || '');  // safe fallback until init()

export function init(page, api) {
  escHtml = api.escHtml;
  // Bind refresh button
  const refreshBtn = page.querySelector('[data-action="refresh"]');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => {
      loadJobAllocation(page, api);
      loadQueueInfo(page, api);
    });
  }

  // Auto-load on init
  loadJobAllocation(page, api);
  loadQueueInfo(page, api);
}

export function onNotification(data, page, api) {
  // Queue info doesn't use notifications
}

// ─────────────────────────────────────────────────────────────
//  Internal functions
// ─────────────────────────────────────────────────────────────

async function loadJobAllocation(page, api) {
  const area = page.querySelector('.qi-allocation-area');
  if (!area) return;

  try {
    const data  = await api.fetch('job_allocation');
    const alloc = data.allocation;

    if (alloc === null || alloc === undefined) {
      area.innerHTML = `
        <div class="card qi-alloc-card">
          <div class="card-title">🖥️ Edge Allocation</div>
          <p style="color:var(--muted)">Running on login node — no active job allocation.</p>
        </div>`;
    } else {
      const runtime = alloc.runtime != null ? formatDuration(alloc.runtime) : 'Unlimited';
      area.innerHTML = `
        <div class="card qi-alloc-card qi-alloc-active">
          <div class="card-title">🖥️ Edge Allocation</div>
          <div class="qi-alloc-grid">
            <span class="qi-alloc-label">Nodes</span>
            <span class="qi-alloc-value">${escHtml(String(alloc.n_nodes))}</span>
            <span class="qi-alloc-label">Walltime limit</span>
            <span class="qi-alloc-value">${escHtml(runtime)}</span>
          </div>
        </div>`;
    }
  } catch (e) {
    area.innerHTML = `
      <div class="card qi-alloc-card">
        <div class="card-title">🖥️ Edge Allocation</div>
        <p style="color:var(--danger)">Error: ${escHtml(e.message)}</p>
      </div>`;
  }
}

async function loadQueueInfo(page, api) {
  const content = page.querySelector('.queueinfo-content');
  content.innerHTML = '<div class="empty"><div class="spinner"></div><p style="margin-top:10px">Fetching queue info…</p></div>';

  try {
    const sid = await api.getSession('queue_info');
    const [info, allocs] = await Promise.all([
      api.fetch(`get_info/${sid}`),
      api.fetch(`list_allocations/${sid}`)
    ]);

    const queuesObj = info.partitions || info.queues || info || {};
    const queues = Array.isArray(queuesObj) ? queuesObj : Object.values(queuesObj);
    const allocations = allocs.allocations || [];
    queueDataCache[api.edgeName] = { queues, allocations };
    api.setQueueData({ queues, allocations });

    content.innerHTML = renderQueueInfo(queues, allocations, api, sid);

    // Bind view jobs buttons
    content.querySelectorAll('[data-action="view-jobs"]').forEach(btn => {
      btn.addEventListener('click', () => {
        const queue = btn.dataset.queue;
        loadQueueJobs(api, sid, queue, btn);
      });
    });

    // Load user's jobs into the inline table
    loadMyJobs(content, api, sid);
  } catch (e) {
    content.innerHTML = `<div class="card"><p style="color:var(--danger)">Error: ${escHtml(e.message)}</p></div>`;
    api.flash('QueueInfo error: ' + e.message, false);
  }
}

function renderQueueInfo(partitions, allocations, api, sid) {
  let html = "";

  if (!Array.isArray(partitions) || !partitions.length) {
    html += `<div class="card"><p style="color:var(--muted)">No partition data returned.</p></div>`;
  } else {
    html += `<div class="card"><div class="card-title">📋 Partitions / Queues</div>
      <table><thead><tr><th>Name</th><th>State</th><th>Nodes</th><th>CPUs</th><th>Jobs</th><th>Actions</th></tr></thead><tbody>`;

    for (const p of partitions) {
      const name = p.name || p.partition || JSON.stringify(p).slice(0, 30);
      const state = p.state || p.avail || '-';
      const stateBadge = state.toLowerCase().includes('up') ? 'badge-green' : 'badge-orange';
      const eName = escHtml(name);
      html += `<tr>
        <td><strong>${eName}</strong></td>
        <td><span class="badge ${stateBadge}">${escHtml(state)}</span></td>
        <td>${p.nodes || p.total_nodes || '-'}</td>
        <td>${p.cpus || p.total_cpus || '-'}</td>
        <td>${p.jobs || '-'}</td>
        <td><button class="btn btn-secondary btn-sm" data-action="view-jobs" data-queue="${eName}">View Jobs</button></td>
      </tr>`;
    }
    html += '</tbody></table></div>';
  }

  if (Array.isArray(allocations) && allocations.length > 0) {
    html += `<div class="card"><div class="card-title">💼 Allocations / Projects</div>
      <table><thead><tr><th>Account</th><th>User</th><th>Fairshare</th><th>QoS</th><th>Max Jobs</th></tr></thead><tbody>`;

    for (const a of allocations) {
      html += `<tr>
        <td><strong>${escHtml(a.account || '-')}</strong></td>
        <td>${escHtml(a.user || '-')}</td>
        <td>${a.fairshare || '-'}</td>
        <td><span class="badge badge-gray">${escHtml(a.qos || '-')}</span></td>
        <td>${a.max_jobs || '-'}</td>
      </tr>`;
    }
    html += '</tbody></table></div>';
  }

  html += '<div id="qi-jobs-area"></div>';
  return html;
}

async function loadQueueJobs(api, sid, queue, btn) {
  btn.disabled = true;
  btn.textContent = '…';

  try {
    const data = await api.fetch(`list_jobs/${sid}/${encodeURIComponent(queue)}?force=true`);
    const jobs = data.jobs || data || [];
    currentJobsData = jobs;

    const title = `📄 Jobs in ${queue}`;
    let body;

    if (!jobs.length) {
      body = `<p style="color:var(--muted)">No jobs in <strong>${queue}</strong>.</p>`;
    } else {
      const CANCELLABLE = new Set(['RUNNING', 'PENDING', 'CONFIGURING', 'SUSPENDED']);

      let html = `<table>
        <thead><tr>
          <th>Job ID</th><th>Name</th><th>User</th><th>State</th><th>Nodes</th><th>Time Used</th><th></th>
        </tr></thead><tbody>`;

      for (const j of jobs) {
        const jobId = j.job_id || j.id || '-';
        const st = j.state || j.job_state || '-';
        const badge = { 'RUNNING': 'badge-green', 'PENDING': 'badge-orange', 'COMPLETED': 'badge-blue', 'FAILED': 'badge-red' }[st] || 'badge-gray';
        const eid = escHtml(jobId);
        const cancelBtn = CANCELLABLE.has(st)
          ? `<button class="btn btn-danger btn-sm cancel-job-btn" data-job-id="${eid}" title="Cancel job ${eid}">✕</button>`
          : '';
        html += `<tr class="qi-job-row" data-job-id="${eid}">
          <td><strong>${eid}</strong></td>
          <td>${escHtml(j.job_name || j.name || '-')}</td>
          <td>${escHtml(j.user || j.user_name || '-')}</td>
          <td><span class="badge ${badge}">${escHtml(st)}</span></td>
          <td>${j.nodes || j.num_nodes || '-'}</td>
          <td>${formatDuration(j.time_used)}</td>
          <td>${cancelBtn}</td>
        </tr>`;
      }
      html += '</tbody></table>';
      html += '<div id="job-detail-panel" class="job-detail-panel" style="display:none;"></div>';
      body = html;
    }

    api.showOverlay(title, body);

    // Bind job row clicks — use dataset (already escaped on write)
    document.querySelectorAll('.qi-job-row').forEach(row => {
      row.addEventListener('click', (e) => {
        if (e.target.closest('.cancel-job-btn')) return;
        showJobDetail(row.dataset.jobId);
      });
    });

    // Bind cancel buttons
    document.querySelectorAll('.cancel-job-btn').forEach(cancelBtn => {
      cancelBtn.addEventListener('click', async (e) => {
        e.stopPropagation();
        const jobId = cancelBtn.dataset.jobId;
        cancelBtn.disabled = true;
        cancelBtn.textContent = '…';
        try {
          await api.fetch(`cancel/${sid}/${encodeURIComponent(jobId)}`, { method: 'POST' });
          api.flash(`Job ${jobId} canceled`);
          cancelBtn.closest('tr').querySelector('.badge').textContent = 'CANCELED';
          cancelBtn.remove();
        } catch (err) {
          api.flash('Cancel failed: ' + err.message, false);
          cancelBtn.disabled = false;
          cancelBtn.textContent = '✕';
        }
      });
    });

  } catch (e) {
    api.flash('Error loading jobs: ' + e.message, false);
  } finally {
    btn.disabled = false;
    btn.textContent = 'View Jobs';
  }
}

function showJobDetail(jobId) {
  const job = currentJobsData.find(j => (j.job_id || j.id) === jobId);
  if (!job) return;

  const detailPanel = document.getElementById('job-detail-panel');
  if (!detailPanel) return;

  const st = job.state || job.job_state || '-';
  const badge = { 'RUNNING': 'badge-green', 'PENDING': 'badge-orange', 'COMPLETED': 'badge-blue', 'FAILED': 'badge-red' }[st] || 'badge-gray';

  detailPanel.innerHTML = `
    <h4>📋 Job Details: ${escHtml(job.job_id || job.id || '-')}</h4>
    <div class="job-detail-grid">
      <div class="job-detail-item">
        <span class="label">Job Name</span>
        <span class="value">${escHtml(job.job_name || job.name || '-')}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">State</span>
        <span class="value"><span class="badge ${badge}">${escHtml(st)}</span></span>
      </div>
      <div class="job-detail-item">
        <span class="label">User</span>
        <span class="value">${escHtml(job.user || job.user_name || '-')}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Partition</span>
        <span class="value">${escHtml(job.partition || '-')}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Account</span>
        <span class="value">${escHtml(job.account || '-')}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Nodes</span>
        <span class="value">${job.nodes || job.num_nodes || '-'}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">CPUs</span>
        <span class="value">${job.cpus || '-'}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Node List</span>
        <span class="value">${escHtml(job.node_list || '-')}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Submit Time</span>
        <span class="value">${formatTimestamp(job.submit_time)}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Start Time</span>
        <span class="value">${formatTimestamp(job.start_time)}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Time Limit</span>
        <span class="value">${job.time_limit ? formatDuration(job.time_limit * 60) : '-'}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Time Used</span>
        <span class="value">${formatDuration(job.time_used)}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Priority</span>
        <span class="value">${job.priority || '-'}</span>
      </div>
    </div>
  `;
  detailPanel.style.display = 'block';

  // Highlight selected row
  document.querySelectorAll('.qi-job-row').forEach(r => r.classList.remove('selected'));
  const selectedRow = document.querySelector(`.qi-job-row[data-job-id="${CSS.escape(jobId)}"]`);
  if (selectedRow) selectedRow.classList.add('selected');
}

async function loadMyJobs(content, api, sid) {
  const area = content.querySelector('#qi-jobs-area');
  if (!area) return;

  area.innerHTML = '<div class="card"><div class="card-title">👤 Jobs</div><div class="empty"><div class="spinner"></div><p style="margin-top:10px">Loading…</p></div></div>';

  try {
    const data = await api.fetch(`list_all_jobs/${sid}?force=true`);
    const jobs = data.jobs || [];
    myJobsData = jobs;

    if (!jobs.length) {
      area.innerHTML = '<div class="card"><div class="card-title">👤 Jobs</div><p style="color:var(--muted)">No active jobs.</p></div>';
      return;
    }

    const CANCELLABLE = new Set(['RUNNING', 'PENDING', 'CONFIGURING', 'SUSPENDED']);
    let html = `<div class="card"><div class="card-title">👤 Jobs</div>
      <table>
        <thead><tr>
          <th>Job ID</th><th>Name</th><th>Partition</th><th>State</th><th>Nodes</th><th>Time Used</th><th></th>
        </tr></thead><tbody>`;

    for (const j of jobs) {
      const jobId = j.job_id || j.id || '-';
      const eid = escHtml(jobId);
      const st = j.state || j.job_state || '-';
      const badge = { 'RUNNING': 'badge-green', 'PENDING': 'badge-orange', 'COMPLETED': 'badge-blue', 'FAILED': 'badge-red' }[st] || 'badge-gray';
      const cancelBtn = CANCELLABLE.has(st)
        ? `<button class="btn btn-danger btn-sm my-cancel-job-btn" data-job-id="${eid}" title="Cancel job ${eid}">✕</button>`
        : '';
      html += `<tr class="qi-job-row qi-my-job-row" data-job-id="${eid}">
        <td><strong>${eid}</strong></td>
        <td>${escHtml(j.job_name || j.name || '-')}</td>
        <td>${escHtml(j.partition || '-')}</td>
        <td><span class="badge ${badge}">${escHtml(st)}</span></td>
        <td>${j.nodes || j.num_nodes || '-'}</td>
        <td>${formatDuration(j.time_used)}</td>
        <td>${cancelBtn}</td>
      </tr>`;
    }
    html += '</tbody></table></div>';
    area.innerHTML = html;

    // Bind row clicks to show detail overlay
    area.querySelectorAll('.qi-my-job-row').forEach(row => {
      row.addEventListener('click', (e) => {
        if (e.target.closest('.my-cancel-job-btn')) return;
        const jobId = row.dataset.jobId;
        showJobDetailOverlay(jobId, api);
      });
    });

    // Bind cancel buttons
    area.querySelectorAll('.my-cancel-job-btn').forEach(cancelBtn => {
      cancelBtn.addEventListener('click', async (e) => {
        e.stopPropagation();
        const jobId = cancelBtn.dataset.jobId;
        cancelBtn.disabled = true;
        cancelBtn.textContent = '…';
        try {
          await api.fetch(`cancel/${sid}/${encodeURIComponent(jobId)}`, { method: 'POST' });
          api.flash(`Job ${jobId} canceled`);
          cancelBtn.closest('tr').querySelector('.badge').textContent = 'CANCELED';
          cancelBtn.remove();
        } catch (err) {
          api.flash('Cancel failed: ' + err.message, false);
          cancelBtn.disabled = false;
          cancelBtn.textContent = '✕';
        }
      });
    });

  } catch (e) {
    area.innerHTML = `<div class="card"><div class="card-title">👤 Jobs</div><p style="color:var(--danger)">Error: ${escHtml(e.message)}</p></div>`;
  }
}

function showJobDetailOverlay(jobId, api) {
  const job = myJobsData.find(j => (j.job_id || j.id) === jobId);
  if (!job) return;

  const st = job.state || job.job_state || '-';
  const badge = { 'RUNNING': 'badge-green', 'PENDING': 'badge-orange', 'COMPLETED': 'badge-blue', 'FAILED': 'badge-red' }[st] || 'badge-gray';

  const body = `
    <div class="job-detail-grid">
      <div class="job-detail-item">
        <span class="label">Job Name</span>
        <span class="value">${escHtml(job.job_name || job.name || '-')}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">State</span>
        <span class="value"><span class="badge ${badge}">${escHtml(st)}</span></span>
      </div>
      <div class="job-detail-item">
        <span class="label">User</span>
        <span class="value">${escHtml(job.user || job.user_name || '-')}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Partition</span>
        <span class="value">${escHtml(job.partition || '-')}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Account</span>
        <span class="value">${escHtml(job.account || '-')}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Nodes</span>
        <span class="value">${job.nodes || job.num_nodes || '-'}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">CPUs</span>
        <span class="value">${job.cpus || '-'}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Node List</span>
        <span class="value">${escHtml(job.node_list || '-')}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Submit Time</span>
        <span class="value">${formatTimestamp(job.submit_time)}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Start Time</span>
        <span class="value">${formatTimestamp(job.start_time)}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Time Limit</span>
        <span class="value">${job.time_limit ? formatDuration(job.time_limit * 60) : '-'}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Time Used</span>
        <span class="value">${formatDuration(job.time_used)}</span>
      </div>
      <div class="job-detail-item">
        <span class="label">Priority</span>
        <span class="value">${job.priority || '-'}</span>
      </div>
    </div>
  `;

  api.showOverlay(`📋 Job Details: ${escHtml(job.job_id || job.id || '-')}`, body);
}

// ─────────────────────────────────────────────────────────────
//  Utility functions
// ─────────────────────────────────────────────────────────────

function formatTimestamp(ts) {
  if (!ts || ts === 0) return '-';
  const date = new Date(ts * 1000);
  if (isNaN(date.getTime())) return '-';
  return date.toLocaleString();
}

function formatDuration(seconds) {
  if (!seconds || seconds <= 0) return '-';
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

