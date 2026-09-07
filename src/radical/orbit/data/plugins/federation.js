/**
 * Federation Plugin Module for ORBIT Explorer
 *
 * One table of joined resources, each followed by one indented row per
 * **member** — a member is one resource shape (a queue plus a pilot size),
 * and it lives in the capability-class pool `fed-<class>`.  The resource row
 * is the aggregate of its members.  Polls `GET resources/default` every 3 s
 * — the plugin refreshes usage on that call (server-side cache: 2 s), so the
 * page needs no other state.
 *
 * A GPU in a member's size is what the operator **declared**, not a device
 * reserved for a task: pilot capacity is task-count based.
 *
 * A record without `members` (a broker that predates class pools) renders
 * exactly as it did before: one row, no sub-rows.
 *
 * All federation routes ride the reserved `default` session, so this module
 * never registers one of its own.
 */

export const name = 'federation';

const POLL_MS = 3000;

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">🌐</div>
      <h2>Federation — <span class="endpoint-label"></span></h2>
      <span class="fed-summary" title="Resource count">…</span>
      <button class="btn btn-secondary btn-sm" style="margin-left:auto" data-action="refresh">↺ Refresh</button>
    </div>
    <div class="fed-content">
      <div class="empty">
        <div class="empty-icon">⏳</div>
        <p>Loading resources…</p>
      </div>
    </div>
  `;
}

export function css() {
  return `
    .fed-summary {
      margin-left: 12px;
      padding: 2px 10px;
      border-radius: 12px;
      font-size: 0.78rem;
      font-weight: 600;
      background: var(--bg2);
      color: var(--muted);
      border: 1px solid var(--border, #ccc);
    }
    .fed-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.85rem;
    }
    .fed-table th,
    .fed-table td {
      padding: 5px 8px;
      text-align: left;
      border-bottom: 1px solid var(--border, #eee);
      white-space: nowrap;
    }
    .fed-table th {
      font-weight: 500;
      color: var(--muted);
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.3px;
    }
    .fed-name { font-weight: 600; }
    .fed-badge {
      padding: 2px 8px;
      border-radius: 10px;
      font-size: 0.72rem;
      background: var(--bg2);
      color: var(--muted);
      border: 1px solid var(--border, #ccc);
    }
    .fed-live-ok      { color: var(--success, #2e7d32); font-weight: 600; }
    .fed-live-suspect { color: var(--warning, #b26a00); font-weight: 600; }
    .fed-live-lost    { color: var(--danger,  #c62828); font-weight: 600; }
    .fed-stale { color: var(--muted); font-style: italic; }
    .fed-bar {
      position: relative;
      display: inline-block;
      width: 90px;
      height: 8px;
      border-radius: 4px;
      background: var(--bg2);
      border: 1px solid var(--border, #ccc);
      vertical-align: middle;
      margin-right: 6px;
      overflow: hidden;
    }
    .fed-bar > span { display: block; height: 100%; background: var(--accent, #4a90d9); }
    .fed-soft { color: var(--muted); font-size: 0.78rem; white-space: normal; }
    .fed-member td { padding-left: 18px; color: var(--muted); }
    .fed-member td:first-child { padding-left: 26px; }
    .fed-empty {
      padding: 12px;
      color: var(--muted);
      font-style: italic;
    }
  `;
}

export async function init(page, api) {
  page.querySelector('[data-action="refresh"]')
      .addEventListener('click', () => loadResources(page, api));
  startPolling(page, api);
  await loadResources(page, api);
}

export async function onShow(page, api) {
  startPolling(page, api);
  await loadResources(page, api);
}

export function onNotification() {}

/* Poll while the page is in the DOM; the timer stops itself once it is not. */
function startPolling(page, api) {
  if (page._fedTimer) return;
  page._fedTimer = setInterval(() => {
    if (!page.isConnected) {
      clearInterval(page._fedTimer);
      page._fedTimer = null;
      return;
    }
    loadResources(page, api);
  }, POLL_MS);
}

async function loadResources(page, api) {
  const content = page.querySelector('.fed-content');
  const summary = page.querySelector('.fed-summary');
  if (!content) return;
  try {
    const r = await api.fetch('resources/default');
    const resources = r.resources || [];
    summary.textContent =
      `${resources.length} resource${resources.length === 1 ? '' : 's'}`;
    content.innerHTML = renderTable(resources, api);
  } catch (e) {
    summary.textContent = '?';
    content.innerHTML =
      `<div class="card"><p style="color:var(--danger)">Error: ${api.escHtml(e.message)}</p></div>`;
    api.flash('Federation: ' + e.message, false);
  }
}

function renderTable(resources, api) {
  if (resources.length === 0) {
    return `<div class="card fed-empty">No resources joined.</div>`;
  }
  const rows = resources.map(r => {
    const members = Array.isArray(r.members) ? r.members : [];
    return renderResourceRow(r, members, api)
         + members.map(m => renderMemberRow(r, m, api)).join('');
  }).join('');
  return `
    <div class="card">
      <table class="fed-table">
        <thead>
          <tr>
            <th>resource</th><th>endpoint</th><th>mode</th><th>site</th>
            <th>cores</th><th>gpus</th><th>mem GB</th><th>software</th>
            <th>node-hours</th><th>pilots</th><th>tasks</th><th>liveness</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

/* The aggregate row.  Its arithmetic is unchanged: the record's
 * `capabilities` and `usage` are the sum over its members. */
function renderResourceRow(r, members, api) {
  const caps  = r.capabilities || {};
  const usage = r.usage || {};
  const soft  = (caps.software || []).map(s => api.escHtml(s)).join(', ');
  const live  = r.liveness || 'lost';

  return `<tr>
    <td class="fed-name">${api.escHtml(r.name || '?')}</td>
    <td><code style="font-size:.78rem">${api.escHtml(r.endpoint || '?')}</code></td>
    <td><span class="fed-badge">${api.escHtml(r.mode || '?')}</span></td>
    <td>${api.escHtml(r.site || '—')}</td>
    <td>${fmtNum(caps.cores)}</td>
    <td>${fmtNum(caps.gpus)}</td>
    <td>${fmtNum(caps.mem_gb)}</td>
    <td class="fed-soft">${soft || '—'}</td>
    <td>${renderHours(usage, api)}</td>
    <td>${usage.pilots_active ?? 0} active</td>
    <td>${renderTasks(usage)}</td>
    <td class="fed-live-${api.escHtml(live)}">${api.escHtml(live)}</td>
  </tr>`;
}

/* One indented row per member: which class pool it joined, the pilot shape
 * it declares, and its own budget and work. */
function renderMemberRow(r, m, api) {
  const usage = m.usage || {};
  const attrs = m.attributes || {};
  const cls   = m['class'] || m.cls || '?';
  const soft  = (m.software || []).map(s => api.escHtml(s)).join(', ');
  const live  = m.liveness || r.liveness || 'lost';

  return `<tr class="fed-member">
    <td>└ ${api.escHtml(m.member || '?')}</td>
    <td><span class="fed-badge">${api.escHtml(cls)}/${api.escHtml(m.pool_name || '?')}</span></td>
    <td>${api.escHtml(m.queue || '—')}</td>
    <td>${api.escHtml(attrs.site || r.site || '—')}</td>
    <td colspan="3">${api.escHtml(sizeOf(m))}</td>
    <td class="fed-soft">${soft || '—'}</td>
    <td>${renderHours(usage, api)}</td>
    <td>${usage.pilots_active ?? 0} active</td>
    <td>${renderTasks(usage)}</td>
    <td class="fed-live-${api.escHtml(live)}">${api.escHtml(live)}</td>
  </tr>`;
}

/* `1x128c+4g` — one pilot of this member.  The GPU count is what the
 * operator declared, not a reservation. */
function sizeOf(m) {
  const nodes = m.nodes ?? 1;
  const cpus  = m.cpus_per_node ?? 0;
  const gpus  = m.gpus_per_node ?? 0;
  return `${nodes}x${cpus}c` + (gpus ? `+${gpus}g` : '');
}

function renderHours(usage, api) {
  const used  = Number(usage.node_hours_used || 0);
  const left  = Number(usage.node_hours_remaining || 0);
  const total = used + left;
  const pct   = total > 0 ? Math.min(100, (used / total) * 100) : 0;
  return `<span class="fed-bar"><span style="width:${pct.toFixed(0)}%"></span></span>
      ${used.toFixed(2)} used · ${left.toFixed(2)} left
      ${usage.stale ? ' <span class="fed-stale">(stale)</span>' : ''}`;
}

function renderTasks(usage) {
  return `${usage.tasks_running ?? 0} run · ${usage.tasks_done ?? 0} done${
    usage.tasks_failed ? ` · ${usage.tasks_failed} failed` : ''}`;
}

function fmtNum(v) {
  return (v === undefined || v === null) ? '—' : String(v);
}
