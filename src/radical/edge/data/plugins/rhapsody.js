/**
 * Rhapsody Plugin Module for Radical Edge Explorer
 *
 * Task execution via Rhapsody backends (local, Dragon, Flux).
 */

export const name = 'rhapsody';

// Buffer for notifications that arrive before task entries are created
const pendingNotifications = {};  // uid -> { data, timestamp }

// Module-level task tracking: uid -> {uid, executable, arguments, state, ...}
let rhTasks = {};
let activePoller = null;

const TERMINAL = new Set(['DONE', 'FAILED', 'CANCELED', 'COMPLETED']);
const CANCELLABLE_NOT = new Set(['DONE', 'FAILED', 'CANCELED', 'COMPLETED']);

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">🎼</div>
      <h2>Rhapsody Tasks — <span class="edge-label"></span></h2>
    </div>
    <div class="card">
      <div class="card-title">📝 Submit Task</div>
      <div class="form-group"><label>Executable</label><input class="rh-exec" type="text" value="/bin/echo" /></div>
      <div class="form-group"><label>Arguments (space-separated)</label><input class="rh-args" type="text" value="hello from rhapsody" /></div>
      <div class="form-group"><label>Backend</label>
        <select class="rh-backends">
          <option value="concurrent">concurrent</option>
          <option value="dragon_v3">dragon_v3</option>
        </select>
      </div>
      <button class="btn btn-success" data-action="submit">▶ Submit Task</button>
    </div>
    <div class="card rh-tasks-card">
      <div class="card-title">📊 Task Monitor</div>
      <div class="rh-table-area"><p style="color:var(--muted)">No tasks submitted yet.</p></div>
    </div>
  `;
}

export function css() {
  return `
    .rh-table-area table { width: 100%; }
    .task-row { cursor: pointer; transition: background 0.15s; }
    .task-row:hover { background: var(--hover); }
    .job-detail-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 8px 16px;
    }
    .job-detail-item { display: flex; flex-direction: column; }
    .job-detail-item .label {
      font-size: 0.75rem; color: var(--muted); text-transform: uppercase;
    }
    .job-detail-item .value { font-weight: 500; }
    .job-output-section { margin-top: 12px; }
    .job-output-section h4 {
      margin: 0 0 4px 0; font-size: 0.85rem; color: var(--muted);
      text-transform: uppercase;
    }
    .job-output-section pre {
      background: rgba(0,0,0,0.3); border: 1px solid var(--border);
      border-radius: 4px; padding: 8px; margin: 0 0 8px 0;
      max-height: 300px; overflow-y: auto; white-space: pre-wrap;
      font-size: 0.8rem; font-family: 'JetBrains Mono', monospace;
    }
    .job-output-section pre.out-stream { color: var(--accent2); }
    .job-output-section pre.err-stream { color: var(--danger); }
  `;
}

export function init(page, api) {
  const submitBtn = page.querySelector('[data-action="submit"]');
  if (submitBtn) {
    submitBtn.addEventListener('click', () => submitTask(page, api));
  }
}

export function onNotification(data, page, api) {
  if (data.topic !== 'task_status') return;

  const uid   = data.data?.uid || '';
  const state = data.data?.state || '?';

  // Update module-level tracking
  if (rhTasks[uid]) {
    Object.assign(rhTasks[uid], data.data);
  }

  // Update table row
  const updated = updateTaskRow(page, uid, state, data.data);

  // Buffer if task entry doesn't exist yet
  if (!updated && uid) {
    pendingNotifications[uid] = { data: data.data, state, timestamp: Date.now() };
  }
}

export const notificationConfig = {
  topic: 'task_status',
  idField: 'uid'
};

// ─────────────────────────────────────────────────────────────
//  Task table rendering
// ─────────────────────────────────────────────────────────────

function ensureTable(page) {
  const area = page.querySelector('.rh-table-area');
  if (!area) return null;

  let table = area.querySelector('table');
  if (!table) {
    area.innerHTML = `<table>
      <thead><tr>
        <th>UID</th><th>Executable</th><th>Backend</th>
        <th>State</th><th></th>
      </tr></thead><tbody></tbody></table>`;
    table = area.querySelector('table');
  }
  return table;
}

function addTaskRow(page, api, task) {
  const table = ensureTable(page);
  if (!table) return;

  const tbody = table.querySelector('tbody');
  const tr = document.createElement('tr');
  tr.className = 'task-row';
  tr.dataset.uid = task.uid;

  const st = task.state || 'SUBMITTED';
  const badge = stateBadge(st);
  const shortExec = (task.executable || '?').split('/').pop();
  const canCancel = !CANCELLABLE_NOT.has(st);

  tr.innerHTML = `
    <td><strong>${escHtml((task.uid || '').slice(0, 16))}…</strong></td>
    <td><code>${escHtml(shortExec)}</code></td>
    <td>${escHtml(task.backend || 'concurrent')}</td>
    <td><span class="badge ${badge}">${st}</span></td>
    <td>${canCancel ? `<button class="btn btn-danger btn-sm rh-cancel-btn" title="Cancel">✕</button>` : ''}</td>
  `;

  // Row click → detail overlay
  tr.addEventListener('click', (e) => {
    if (e.target.closest('.rh-cancel-btn')) return;
    openTaskDetail(api, task.uid);
  });

  // Cancel button — uses uid from closure, not from data attribute
  const cancelBtn = tr.querySelector('.rh-cancel-btn');
  if (cancelBtn) {
    cancelBtn.addEventListener('click', async (e) => {
      e.stopPropagation();
      cancelBtn.disabled = true;
      cancelBtn.textContent = '…';
      try {
        const sid = await api.getSession('rhapsody');
        await api.fetch(`cancel/${sid}/${encodeURIComponent(task.uid)}`, { method: 'POST' });
        api.flash(`Task ${task.uid.slice(0, 12)}… canceled`);
      } catch (err) {
        api.flash('Cancel failed: ' + err.message, false);
        cancelBtn.disabled = false;
        cancelBtn.textContent = '✕';
      }
    });
  }

  tbody.insertBefore(tr, tbody.firstChild);
}

function updateTaskRow(page, uid, state, data) {
  const row = page.querySelector(`.task-row[data-uid="${CSS.escape(uid)}"]`);
  if (!row) return false;

  const badge = row.querySelector('.badge');
  if (badge) {
    badge.textContent = state;
    badge.className = `badge ${stateBadge(state)}`;
  }

  if (TERMINAL.has(state.toUpperCase())) {
    const cancelBtn = row.querySelector('.rh-cancel-btn');
    if (cancelBtn) cancelBtn.remove();
  }

  return true;
}

// ─────────────────────────────────────────────────────────────
//  Task detail overlay with polling
// ─────────────────────────────────────────────────────────────

async function openTaskDetail(api, uid) {
  stopPoller();

  const sid = await api.getSession('rhapsody');

  let task;
  try {
    task = await api.fetch(`task/${sid}/${uid}`);
  } catch (e) {
    api.flash('Error loading task details: ' + e.message, false);
    return;
  }

  // Update module-level cache
  if (rhTasks[uid]) Object.assign(rhTasks[uid], task);

  renderDetailOverlay(api, task);

  // Poll for updates if non-terminal
  const stateUpper = (task.state || '').toUpperCase();
  if (!TERMINAL.has(stateUpper)) {
    activePoller = setInterval(async () => {
      const overlay = document.getElementById('jobs-overlay');
      if (!overlay || !overlay.classList.contains('visible')) {
        stopPoller();
        return;
      }

      try {
        const upd = await api.fetch(`task/${sid}/${uid}`);

        // Update state badge
        const stateEl = document.getElementById('rh-detail-state');
        if (stateEl) {
          stateEl.className = `badge ${stateBadge(upd.state || '')}`;
          stateEl.textContent = upd.state || '?';
        }

        // Update exit code
        const rc = upd.exit_code ?? upd.retval;
        if (rc != null) {
          const rcEl = document.getElementById('rh-detail-rc');
          if (rcEl) rcEl.textContent = rc;
        }

        // Update stdout/stderr (replace, not append — Rhapsody returns full content)
        if (upd.stdout) {
          const outEl = document.getElementById('rh-detail-stdout');
          if (outEl) outEl.textContent = upd.stdout;
        }
        if (upd.stderr) {
          const errEl = document.getElementById('rh-detail-stderr');
          if (errEl) errEl.textContent = upd.stderr;
        }
        if (upd.exception) {
          const excEl = document.getElementById('rh-detail-exception');
          if (excEl) {
            excEl.textContent = upd.exception;
            excEl.parentElement.style.display = '';
          }
        }

        if (TERMINAL.has((upd.state || '').toUpperCase())) {
          stopPoller();
          if (rhTasks[uid]) Object.assign(rhTasks[uid], upd);
        }
      } catch (e) {
        // Silently ignore poll errors
      }
    }, 3000);
  }
}

function stopPoller() {
  if (activePoller) {
    clearInterval(activePoller);
    activePoller = null;
  }
}

function renderDetailOverlay(api, task) {
  const st = task.state || '-';
  const badge = stateBadge(st);
  const argsStr = Array.isArray(task.arguments) ? task.arguments.join(' ') : (task.arguments || '-');

  const fields = [
    ['UID',        escHtml(task.uid || '-')],
    ['State',      `<span id="rh-detail-state" class="badge ${badge}">${st}</span>`],
    ['Exit Code',  `<span id="rh-detail-rc">${task.exit_code ?? task.retval ?? '-'}</span>`],
    ['Executable', escHtml(task.executable || '-')],
    ['Arguments',  `<code>${escHtml(argsStr)}</code>`],
    ['Backend',    escHtml(task.backend || '-')],
  ];

  let body = '<div class="job-detail-grid">';
  for (const [label, value] of fields) {
    body += `<div class="job-detail-item">
      <span class="label">${label}</span>
      <span class="value">${value}</span>
    </div>`;
  }
  body += '</div>';

  // stdout / stderr / exception sections
  const hasException = task.exception;
  body += `
    <div class="job-output-section">
      <h4>stdout</h4>
      <pre id="rh-detail-stdout" class="out-stream">${escHtml(task.stdout || '')}</pre>
    </div>
    <div class="job-output-section">
      <h4>stderr</h4>
      <pre id="rh-detail-stderr" class="err-stream">${escHtml(task.stderr || '')}</pre>
    </div>
    <div class="job-output-section" style="${hasException ? '' : 'display:none'}">
      <h4>exception</h4>
      <pre id="rh-detail-exception" class="err-stream">${escHtml(task.exception || '')}</pre>
    </div>
  `;

  const title = `🎼 Task Details: ${escHtml((task.uid || '').slice(0, 16))}…`;
  api.showOverlay(title, body);
}

// ─────────────────────────────────────────────────────────────
//  Submit
// ─────────────────────────────────────────────────────────────

async function submitTask(page, api) {
  const exec = page.querySelector('.rh-exec').value.trim();
  const args = page.querySelector('.rh-args').value.trim().split(/\s+/).filter(Boolean);
  const backendsRaw = page.querySelector('.rh-backends').value.trim();
  const backends = backendsRaw ? backendsRaw.split(',').map(s => s.trim()).filter(Boolean) : null;

  try {
    const regBody = backends ? { backends } : {};
    const sid = await api.getSession('rhapsody', regBody);

    const tasks = [{ executable: exec, arguments: args }];
    const submitted = await api.fetch(`submit/${sid}`, {
      method: 'POST',
      body: JSON.stringify({ tasks })
    });

    const taskList = (Array.isArray(submitted) ? submitted : submitted.tasks || [submitted]);
    const uids = taskList.map(t => t.uid).filter(Boolean);

    api.flash(`Rhapsody task(s) submitted: ${uids.join(', ')}`);

    for (const uid of uids) {
      api.registerTask('rhapsody', uid, `${exec} ${args.join(' ')}`);

      const taskData = {
        uid:        uid,
        executable: exec,
        arguments:  args,
        backend:    backends ? backends[0] : 'concurrent',
        state:      'SUBMITTED',
      };
      rhTasks[uid] = taskData;

      addTaskRow(page, api, taskData);

      // Check for pending notifications
      if (pendingNotifications[uid]) {
        const pending = pendingNotifications[uid];
        updateTaskRow(page, uid, pending.state, pending.data);
        if (rhTasks[uid]) Object.assign(rhTasks[uid], pending.data);
        delete pendingNotifications[uid];
      }
    }

  } catch (e) {
    api.flash('Rhapsody error: ' + e.message, false);
  }
}

// ─────────────────────────────────────────────────────────────
//  Utility functions
// ─────────────────────────────────────────────────────────────

function stateBadge(state) {
  const s = (state || '').toUpperCase();
  if (['DONE', 'COMPLETED'].includes(s)) return 'badge-green';
  if (['RUNNING', 'ACTIVE'].includes(s)) return 'badge-blue';
  if (['FAILED', 'ERROR', 'CANCELED'].includes(s)) return 'badge-red';
  return 'badge-orange';
}

function escHtml(s) {
  if (!s) return '';
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
