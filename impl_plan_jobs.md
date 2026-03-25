# Implementation Plan: Code Review & Documentation Overhaul

## Goal

Address findings from comprehensive review of Python backend, JS UI, and
documentation after the job/task table+overlay feature landed.

---

## Priority 1 — Security & Correctness

### 1.1 XSS in JS plugin data attributes
- [x] `psij.js`: escape `job.job_id` in `data-jobid="${job.job_id}"` (user-controlled via PSI/J)
- [x] `rhapsody.js`: escape `task.uid` in `data-uid="${task.uid}"`
- [x] `queue_info.js`: escape `job.job_id` in `data-jobid="${job.job_id}"`
- [x] Audit all `innerHTML` assignments for unescaped interpolations; use `escHtml()` consistently
- **commit**: `27872b4`

### 1.2 SSL verification disabled
- [x] N/A — `verify=False` only used as fallback when no cert is configured
  (client.py, plugin_xgfabric.py); by-design for dev mode

### 1.3 Race conditions in backend
- [x] `plugin_psij.py`: `_job_meta` and `_jobs` — all access is from async
  event loop (no thread callbacks); `list()` copy used in poll loop — safe
- [x] `plugin_rhapsody.py`: `_notified_states` dict written from backend callback
  thread and read from async context — added `threading.Lock`
- [x] Route caching: review agent finding was inaccurate, no such caching exists
- **commit**: `(next)`

---

## Priority 2 — UI Quality

### 2.1 CSS class collisions across plugins
- [ ] `.job-row` defined in `psij.js` CSS, conflicts with `queue_info.js` `.job-row`
- [ ] `.job-detail-grid`, `.job-detail-item`, `.job-output-section` duplicated
  across all three plugin CSS blocks
- [ ] **Fix**: either namespace classes (`.psij-job-row`, `.rh-task-row`,
  `.qi-job-row`) or extract shared overlay/detail CSS into `edge_explorer.html`

### 2.2 Event listener leaks
- [ ] Each call to `openJobDetail()` / `openTaskDetail()` creates a new
  `setInterval` — the old poller is stopped, but overlay DOM event listeners
  from previous opens are never removed
- [ ] Row click listeners in `addJobRow()` / `addTaskRow()` accumulate if rows
  are re-rendered — use event delegation on `<tbody>` instead

### 2.3 Null-safety in poll callbacks
- [ ] `psij.js` poll: `document.getElementById('psij-detail-state')` can be
  `null` if overlay was closed between fetch start and response
- [ ] Same pattern in `rhapsody.js` — guard all `getElementById` results

---

## Priority 3 — Documentation

### 3.1 Outdated / broken docs
- [ ] `docs/source/plugin_development.rst`: references `PluginBase`,
  `SessionBase` which don't exist — rewrite to match actual `Plugin`,
  `PluginSession` classes
- [ ] `examples/example_rose.py`: imports `RoseClient` which doesn't exist —
  either delete or update to match current plugin names

### 3.2 Missing documentation
- [ ] **REST API reference**: document all plugin endpoints with method, path,
  request/response JSON schemas (bridge proxy endpoints + per-plugin routes)
- [ ] **Configuration guide**: document bridge/edge CLI args, environment
  variables, config file format
- [ ] **Integrator / AAA guide**: document how to add authentication,
  authorization, and accounting — middleware hooks, session validation, etc.
- [ ] **Plugin developer guide** (replace outdated `.rst`): cover `Plugin` base
  class, `PluginSession`, route registration, notification sending, `ui_config`
  schema, JS module API (`api.showOverlay`, `api.fetch`, `api.getSession`, etc.)

### 3.3 Sync CLAUDE.md with new APIs
- [ ] Add `list_jobs` / `list_tasks` methods to CLAUDE.md plugin descriptions
- [ ] Add `get_task` endpoint to rhapsody plugin description
- [ ] Document byte-offset stdout/stderr streaming for PsiJ
- [ ] Document notification callback registration for Rhapsody backends

### 3.4 Sync examples with code
- [ ] Verify `example_psij.py` uses current `PSIJClient` API (including new
  `list_jobs()`, offset params)
- [ ] Verify `example_rhapsody.py` uses current `RhapsodyClient` API (including
  `list_tasks()`, `get_task()`)
- [ ] Verify `example_sysinfo.py` still works

---

## Priority 4 — Polish & Minor Issues

### 4.1 Backend warnings
- [ ] `plugin_psij.py`: `_read_output_file()` silently returns `''` on any
  exception — log at DEBUG level
- [ ] `plugin_rhapsody.py`: `_watch_task` catches all exceptions — ensure
  error notification always fires (currently does, but verify edge cases)
- [ ] `plugin_queue_info.py`: `_collect_all_user_jobs()` runs `squeue --json`
  which can be slow on large clusters — consider adding timeout

### 4.2 PsiJ polling termination
- [ ] Poll loop checks `TERMINAL` set but PsiJ has intermediate states
  (`STAGE_OUT`, `CLEANUP`) that precede `COMPLETED` — verify the JS
  `TERMINAL` set matches the Python side exactly

### 4.3 Test coverage
- [ ] Add unit tests for new `list_jobs` / `list_tasks` endpoints
- [ ] Add unit tests for byte-offset stdout/stderr reading
- [ ] Add unit tests for Rhapsody callback registration

---

## Progress log

_(to be filled as items are completed)_
