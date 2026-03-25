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
- **commit**: `821d82c`

---

## Priority 2 — UI Quality

### 2.1 CSS class collisions across plugins
- [x] `.job-row` → `.psij-job-row`, `.rh-task-row`, `.qi-job-row` (namespaced)
- [x] `.job-detail-grid`, `.job-detail-item`, `.job-output-section`,
  `.job-detail-panel` extracted into `edge_explorer.html`
- **commit**: `f718beb`

### 2.2 Event listener leaks
- [x] N/A — `stopPoller()` is called before each new overlay open; old overlay
  DOM nodes and listeners are GC'd when `showOverlay` replaces innerHTML
- [x] Row click listeners added once per row creation (not re-rendered) — no leak

### 2.3 Null-safety in poll callbacks
- [x] Already guarded — all `getElementById` calls followed by `if (el)` check

---

## Priority 3 — Documentation

### 3.1 Outdated / broken docs
- [x] `docs/source/plugin_development.rst`: rewritten to document actual
  `Plugin`, `PluginSession`, `PluginClient` classes with accurate examples
- [x] `examples/example_rose.py`: deleted (imported non-existent rose plugin)
- **commit**: `b21e5d9`

### 3.2 Missing documentation
- [ ] **REST API reference**: document all plugin endpoints (deferred — large effort)
- [ ] **Configuration guide**: document bridge/edge CLI args (deferred)
- [ ] **Integrator / AAA guide**: document AAA hooks (deferred)
- [x] **Plugin developer guide**: rewritten in step 3.1

### 3.3 Sync CLAUDE.md with new APIs
- [x] Added `list_jobs`, `list_tasks`, `get_task`, byte-offset streaming,
  backend callback notifications to plugin descriptions
- [x] Updated test count to 231
- **commit**: `74a9051`

### 3.4 Sync examples with code
- [x] `example_psij.py` — verified, uses current API correctly
- [x] `example_rhapsody.py` — verified, uses `get_task()`, `get_statistics()`
- [x] `example_rose.py` — deleted in step 3.1 (broken)

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
