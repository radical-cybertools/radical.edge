/**
 * Rhapsody Plugin Module for Radical Edge Explorer
 *
 * Task execution via Rhapsody backends (local, Dragon, Flux).
 */

export const name = 'rhapsody';

// Buffer for notifications that arrive before task entries are created
const pendingNotifications = {};  // uid -> { data, timestamp }

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
      <div class="monitor-area rh-output">No tasks submitted yet.</div>
    </div>
  `;
}

export function css() {
  return `
    /* Monitor area - shared with psij */
    .monitor-area {
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 4px 8px;
      font-family: 'JetBrains Mono', monospace;
      font-size: .8rem;
      max-height: 400px;
      overflow-y: auto;
      color: var(--text-dim);
      min-height: 24px;
    }
    .monitor-area .ok { color: var(--accent2); }
    .monitor-area .err { color: var(--danger); }
    .monitor-area .info { color: var(--accent); }

    /* Task entries in monitor */
    .task-entry {
      margin: 0;
      padding: 0;
      border-bottom: 1px solid rgba(255, 255, 255, 0.03);
    }
    .task-entry:last-child { border-bottom: none; }
    .task-entry > summary {
      display: list-item;
      list-style-position: outside;
      margin-left: 18px;
      cursor: pointer;
      padding: 4px 0;
      user-select: none;
      font-size: 0.85em;
      outline: none;
    }
    .task-entry > summary > .task-summary-content {
      display: inline-flex;
      align-items: center;
      vertical-align: top;
      width: 100%;
      margin-left: -4px;
    }
    .task-entry > .task-log {
      margin: 0 0 4px 18px;
      padding: 4px 0 4px 10px;
      border-left: 2px solid var(--border);
      font-size: 0.82em;
      line-height: 1.2;
    }
    .task-entry > .task-log pre {
      margin: 0;
      padding: 0;
      white-space: pre-wrap;
      background: transparent;
      border: none;
      font-family: 'JetBrains Mono', monospace;
    }

    /* Cancel button */
    .task-cancel-btn {
      background: transparent;
      border: none;
      color: var(--muted);
      cursor: pointer;
      font-size: 0.9em;
      padding: 2px 6px;
      margin-left: auto;
      border-radius: 4px;
      transition: color 0.2s, background 0.2s;
      flex-shrink: 0;
    }
    .task-cancel-btn:hover {
      color: var(--danger);
      background: rgba(255, 77, 109, 0.15);
    }
    .task-cancel-btn:disabled {
      opacity: 0.3;
      cursor: not-allowed;
    }
  `;
}

export function init(page, api) {
  // Bind submit button
  const submitBtn = page.querySelector('[data-action="submit"]');
  if (submitBtn) {
    submitBtn.addEventListener('click', () => submitTask(page, api));
  }
}

export function onNotification(data, page, api) {
  if (data.topic !== 'task_status') return;

  const uid = data.data?.uid || '';
  const state = data.data?.state || '?';

  // Try to apply the notification; if task entry doesn't exist yet, buffer it
  const applied = applyNotification(page, api.edgeName, uid, state, data.data);
  if (!applied && uid) {
    pendingNotifications[uid] = { data: data.data, state, timestamp: Date.now() };
  }
}

// Notification config for the core explorer
export const notificationConfig = {
  topic: 'task_status',
  idField: 'uid'
};

// ─────────────────────────────────────────────────────────────
//  Internal functions
// ─────────────────────────────────────────────────────────────

async function submitTask(page, api) {
  const output = page.querySelector('.rh-output');

  const exec = page.querySelector('.rh-exec').value.trim();
  const args = page.querySelector('.rh-args').value.trim().split(/\s+/).filter(Boolean);
  const backendsRaw = page.querySelector('.rh-backends').value.trim();
  const backends = backendsRaw ? backendsRaw.split(',').map(s => s.trim()).filter(Boolean) : null;

  // Clear placeholder
  if (output.textContent === 'No tasks submitted yet.') output.innerHTML = '';

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

      const taskEntry = document.createElement('details');
      taskEntry.className = 'task-entry';
      taskEntry.id = `rh-task-${api.edgeName}-${uid}`;
      taskEntry.innerHTML = `
        <summary>
          <span class="task-summary-content">
            <strong style="margin-right:6px;">🎼 ${uid.slice(0, 12)}…</strong>
            <span class="rh-task-state badge badge-orange" style="font-size:.65rem;padding:1px 4px;">PENDING</span>
            <code style="font-size:0.8em;color:var(--muted);margin-left:8px;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${exec} ${args.join(' ')}</code>
            <button class="task-cancel-btn" title="Cancel task">✕</button>
          </span>
        </summary>
        <div class="task-log rh-task-log"><div style="color:var(--muted);font-style:italic;">Waiting…</div></div>`;

      // Bind cancel button
      taskEntry.querySelector('.task-cancel-btn').addEventListener('click', (e) => {
        e.stopPropagation();
        cancelTask(page, api, uid, e.target);
      });

      output.appendChild(taskEntry);
      output.scrollTop = output.scrollHeight;

      // Check for pending notifications that arrived before the task entry was created
      if (pendingNotifications[uid]) {
        const pending = pendingNotifications[uid];
        applyNotification(page, api.edgeName, uid, pending.state, pending.data);
        delete pendingNotifications[uid];
      }
    }

  } catch (e) {
    output.innerHTML += `<span class="err">Submit error: ${escHtml(e.message)}</span>\n`;
    api.flash('Rhapsody error: ' + e.message, false);
  }
}

async function cancelTask(page, api, uid, btn) {
  btn.disabled = true;

  try {
    const sid = await api.getSession('rhapsody');
    await api.fetch(`cancel/${sid}/${uid}`, { method: 'POST' });
    api.flash(`Task ${uid.slice(0, 12)}… canceled`);

    // Update the state badge
    const entry = document.getElementById(`rh-task-${api.edgeName}-${uid}`);
    if (entry) {
      const badge = entry.querySelector('.rh-task-state');
      if (badge) {
        badge.textContent = 'CANCELED';
        badge.className = 'rh-task-state badge badge-red';
      }
    }
  } catch (e) {
    api.flash('Cancel failed: ' + e.message, false);
    btn.disabled = false;
  }
}

function applyNotification(page, edgeName, uid, state, data) {
  const entryId = `rh-task-${edgeName}-${uid}`;
  const entry = document.getElementById(entryId);
  if (!entry) return false;

  const stateEl = entry.querySelector('.rh-task-state');
  const logEl = entry.querySelector('.rh-task-log');
  if (!stateEl || !logEl) return false;

  const ts = new Date().toLocaleTimeString();
  const stateUpper = (state || '').toUpperCase();
  const isOk = ['DONE', 'COMPLETED'].some(s => stateUpper.includes(s));
  const isRunning = stateUpper.includes('RUNNING') || stateUpper.includes('ACTIVE');
  const isFailed = ['FAILED', 'ERROR', 'CANCELED'].some(s => stateUpper.includes(s));

  stateEl.className = `rh-task-state badge ${isOk ? 'badge-green' : isRunning ? 'badge-blue' : isFailed ? 'badge-red' : 'badge-orange'}`;
  stateEl.textContent = state;

  // Disable cancel button on terminal states
  const isTerminal = isOk || isFailed;
  const cancelBtn = entry.querySelector('.task-cancel-btn');
  if (cancelBtn && isTerminal) {
    cancelBtn.disabled = true;
  }

  const rc = data.exit_code ?? data.retval ?? '?';
  let logHtml = `<span style="color:var(--muted);font-size:0.9em;">[${ts}] <b>${state}</b> rc:${rc}</span>`;
  if (data.stdout) logHtml += `<pre class="ok">out: ${escHtml(data.stdout).trim()}</pre>`;
  if (data.stderr) logHtml += `<pre class="err">err: ${escHtml(data.stderr).trim()}</pre>`;
  if (data.exception) logHtml += `<pre class="err">exc: ${escHtml(data.exception).trim()}</pre>`;
  logEl.innerHTML = logHtml;

  return true;
}

function escHtml(s) {
  if (!s) return '';
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
