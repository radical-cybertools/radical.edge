/**
 * PsiJ Plugin Module for Radical Edge Explorer
 *
 * HPC job submission via PsiJ (supports local, SLURM, PBS, LSF).
 */

export const name = 'psij';

// Per-edge child counters for generating unique edge names
const edgeCounters = {};

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">🚀</div>
      <h2>PsiJ Jobs — <span class="edge-label"></span></h2>
    </div>
    <div class="card">
      <div class="card-title">📝 Submit Job</div>
      <div class="grid2">
        <div>
          <div class="form-group"><label>Executable</label><input class="p-exec" type="text" value="radical-edge-wrapper.sh" /></div>
          <div class="form-group"><label>Arguments (space-separated)</label><input class="p-args" type="text" value="" placeholder="auto-filled with --url and --name" /></div>
          <div class="form-group"><label>Executor</label>
            <select class="p-executor">
              <option value="local">local</option>
              <option value="slurm">slurm</option>
              <option value="pbs">pbs</option>
              <option value="lsf">lsf</option>
            </select>
          </div>
        </div>
        <div>
          <div class="form-group"><label>Queue / Partition</label><input class="p-queue" type="text" placeholder="optional" /></div>
          <div class="form-group"><label>Account / Project</label><input class="p-account" type="text" placeholder="optional" /></div>
          <div class="form-group"><label>Duration (seconds)</label><input class="p-duration" type="text" placeholder="e.g. 600" /></div>
          <div class="form-group"><label>Number of Nodes</label><input class="p-node-count" type="number" placeholder="e.g. 1" /></div>
          <div class="form-group"><label>🔧 Custom Attributes</label>
            <div class="psij-attributes-container" style="margin-bottom: 4px;">
              <div class="psij-attribute-rows"></div>
              <button type="button" class="btn btn-secondary btn-sm" data-action="add-attr" style="margin-top:8px;">➕ Add Attribute</button>
            </div>
          </div>
        </div>
      </div>
      <button class="btn btn-success" data-action="submit">🚀 Submit Job</button>
    </div>
    <div class="card psij-jobs-card">
      <div class="card-title">📊 Job Monitor</div>
      <div class="monitor-area psij-output">No jobs submitted yet.</div>
    </div>
  `;
}

export function css() {
  return `
    /* Monitor area - shared with rhapsody */
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

    /* Task entries in job monitor */
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
  // Bind add attribute button
  const addAttrBtn = page.querySelector('[data-action="add-attr"]');
  if (addAttrBtn) {
    addAttrBtn.addEventListener('click', () => addAttributeRow(page));
  }

  // Bind submit button
  const submitBtn = page.querySelector('[data-action="submit"]');
  if (submitBtn) {
    submitBtn.addEventListener('click', () => submitJob(page, api));
  }

  // Pre-fill --url / --name args for first submission
  const argsInput = page.querySelector('.p-args');
  if (argsInput && !argsInput.value) {
    const nextName = getNextEdgeChildName(api.edgeName);
    const plugins  = api.getPluginNames().join(',');
    argsInput.value = `--url ${api.bridgeUrl} --name ${nextName} -p ${plugins}`;
  }

  // Prefill from cached queue data if already available
  const qd = api.getQueueData();
  if (qd) replaceQueueAccountDropdowns(page, qd);
}

export function onShow(page, api) {
  // Refresh queue/account dropdowns from latest cache each time the page is shown
  const qd = api.getQueueData();
  if (qd) replaceQueueAccountDropdowns(page, qd);
}

export function onNotification(data, page, api) {
  if (data.topic !== 'job_status') return;

  const jobId = data.data?.job_id || '';
  const state = data.data?.state || '?';

  applyNotification(page, api.edgeName, jobId, state, data.data);
}

// Notification config for the core explorer
export const notificationConfig = {
  topic: 'job_status',
  idField: 'job_id'
};

// ─────────────────────────────────────────────────────────────
//  Internal functions
// ─────────────────────────────────────────────────────────────

function getNextEdgeChildName(edgeName) {
  if (!edgeCounters[edgeName]) edgeCounters[edgeName] = 0;
  edgeCounters[edgeName]++;
  return `${edgeName}.${edgeCounters[edgeName]}`;
}

function replaceQueueAccountDropdowns(page, queueData) {
  const { queues = [], allocations = [] } = queueData;

  // Replace queue text input with a <select> populated from partitions
  const queueInput = page.querySelector('.p-queue');
  if (queueInput && queueInput.tagName === 'INPUT') {
    const sel = document.createElement('select');
    sel.className = queueInput.className;

    const getQName = q => q.name || q.partition || String(q);
    const pb       = queues.find(q => getQName(q) === 'debug');
    const pi       = queues.find(q => getQName(q) === 'interactive');
    const defaultQ = pb || pi || queues[0];
    const defaultQName = defaultQ ? getQName(defaultQ) : '';
    const currentVal   = queueInput.value;

    sel.innerHTML = '<option value="">(none)</option>' + queues.map(q => {
      const qn         = getQName(q);
      const isSelected = (currentVal && currentVal === qn) || (!currentVal && qn === defaultQName);
      return `<option value="${qn}" ${isSelected ? 'selected' : ''}>${qn}</option>`;
    }).join('');
    queueInput.parentNode.replaceChild(sel, queueInput);
  }

  // Replace account text input with a <select> populated from allocations
  const accountInput = page.querySelector('.p-account');
  if (accountInput && accountInput.tagName === 'INPUT') {
    const sel      = document.createElement('select');
    sel.className  = accountInput.className;

    const accounts   = [...new Set(allocations.map(a => a.account).filter(Boolean))];
    const defaultAcc = accounts[0] || '';
    const currentVal = accountInput.value;

    sel.innerHTML = '<option value="">(none)</option>' + accounts.map(a => {
      const isSelected = (currentVal && currentVal === a) || (!currentVal && a === defaultAcc);
      return `<option value="${a}" ${isSelected ? 'selected' : ''}>${a}</option>`;
    }).join('');
    accountInput.parentNode.replaceChild(sel, accountInput);
  }
}

function addAttributeRow(page) {
  const container = page.querySelector('.psij-attribute-rows');
  const row = document.createElement('div');
  row.style.display = 'flex';
  row.style.gap = '10px';
  row.style.marginBottom = '8px';
  row.innerHTML = `
    <input class="p-attr-key" type="text" placeholder="Key (e.g. slurm.constraint)" style="flex:1;" />
    <input class="p-attr-val" type="text" placeholder="Value (e.g. cpu_gen_1)" style="flex:2;" />
    <button class="btn btn-secondary btn-sm" style="padding: 4px 10px;">❌</button>
  `;
  row.querySelector('button').addEventListener('click', () => row.remove());
  container.appendChild(row);
}

async function submitJob(page, api) {
  const output = page.querySelector('.psij-output');

  const exec = page.querySelector('.p-exec').value.trim();
  const args = page.querySelector('.p-args').value.trim().split(/\s+/).filter(Boolean);
  const executor = page.querySelector('.p-executor').value;
  const queue = page.querySelector('.p-queue').value.trim();
  const account = page.querySelector('.p-account').value.trim();
  const duration = page.querySelector('.p-duration').value.trim();
  const nodeCountEl = page.querySelector('.p-node-count');
  const nodeCount = nodeCountEl ? nodeCountEl.value.trim() : '';

  const job_spec = { executable: exec, arguments: args, attributes: {} };
  if (queue) job_spec.attributes.queue_name = queue;
  if (account) job_spec.attributes.account = account;
  if (duration) job_spec.attributes.duration = duration;
  if (nodeCount) job_spec.attributes.node_count = parseInt(nodeCount, 10);

  const attrRows = page.querySelectorAll('.psij-attribute-rows > div');
  attrRows.forEach(row => {
    const key = row.querySelector('.p-attr-key').value.trim();
    const val = row.querySelector('.p-attr-val').value.trim();
    if (key && val) {
      if (!job_spec.custom_attributes) job_spec.custom_attributes = {};
      job_spec.custom_attributes[key] = val;
    }
  });

  // Clear placeholder
  if (output.textContent === 'No jobs submitted yet.') output.innerHTML = '';

  try {
    const sid = await api.getSession('psij');

    const res = await api.fetch(`submit/${sid}`, {
      method: 'POST',
      body: JSON.stringify({ job_spec, executor })
    });

    const jobId = res.job_id;
    api.flash(`Job submitted: ${jobId}`);
    api.registerTask('psij', jobId, `${exec} ${args.join(' ')}`);

    // Create a per-job expandable entry
    const jobEntry = document.createElement('details');
    jobEntry.className = 'task-entry';
    jobEntry.id = `psij-job-${api.edgeName}-${jobId}`;
    jobEntry.innerHTML = `
      <summary>
        <span class="task-summary-content">
          <strong style="margin-right:6px;">🚀 ${jobId.slice(0, 8)}…</strong>
          <span class="psij-job-state badge badge-orange" style="font-size:.65rem;padding:1px 4px;">PENDING</span>
          <code style="font-size:0.8em;color:var(--muted);margin-left:8px;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${exec} ${args.join(' ')}</code>
          <button class="task-cancel-btn" title="Cancel job">✕</button>
        </span>
      </summary>
      <div class="task-log psij-job-log"><div style="color:var(--muted);font-style:italic;">Waiting…</div></div>`;

    // Bind cancel button
    jobEntry.querySelector('.task-cancel-btn').addEventListener('click', (e) => {
      e.stopPropagation();
      cancelJob(page, api, jobId, e.target);
    });

    output.appendChild(jobEntry);
    output.scrollTop = output.scrollHeight;

    // Check for pending notifications
    api.processPendingNotification('psij', jobId);

    // Update the args field for the NEXT submission
    const argsInput = page.querySelector('.p-args');
    const nextName  = getNextEdgeChildName(api.edgeName);
    const plugins   = api.getPluginNames().join(',');
    argsInput.value = `--url ${api.bridgeUrl} --name ${nextName} -p ${plugins}`;

  } catch (e) {
    output.innerHTML += `<span class="err">Submit error: ${api.escHtml(e.message)}</span>\n`;
    api.flash('PsiJ error: ' + e.message, false);
  }
}

async function cancelJob(page, api, jobId, btn) {
  btn.disabled = true;

  try {
    const sid = await api.getSession('psij');
    await api.fetch(`cancel/${sid}/${jobId}`, { method: 'POST' });
    api.flash(`Job ${jobId.slice(0, 8)}… canceled`);

    // Update the state badge
    const entry = page.querySelector(`#psij-job-${api.edgeName}-${jobId}`);
    if (entry) {
      const badge = entry.querySelector('.psij-job-state');
      if (badge) {
        badge.textContent = 'CANCELED';
        badge.className = 'psij-job-state badge badge-red';
      }
    }
  } catch (e) {
    api.flash('Cancel failed: ' + e.message, false);
    btn.disabled = false;
  }
}

function applyNotification(page, edgeName, jobId, state, data) {
  const entryId = `psij-job-${edgeName}-${jobId}`;
  const entry = page.querySelector(`#${entryId}`);
  if (!entry) return false;

  const stateEl = entry.querySelector('.psij-job-state');
  const logEl = entry.querySelector('.psij-job-log');
  if (!stateEl || !logEl) return false;

  const ts = new Date().toLocaleTimeString();
  const stateUpper = (state || '').toUpperCase();
  const isOk = ['DONE', 'COMPLETED'].some(s => stateUpper.includes(s));
  const isRunning = stateUpper.includes('RUNNING') || stateUpper.includes('ACTIVE');
  const isFailed = ['FAILED', 'ERROR', 'CANCELED'].some(s => stateUpper.includes(s));

  stateEl.className = `psij-job-state badge ${isOk ? 'badge-green' : isRunning ? 'badge-blue' : isFailed ? 'badge-red' : 'badge-orange'}`;
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
