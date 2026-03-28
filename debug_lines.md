# Debug lines to remove after Frontier investigation

All debug blocks are marked with `# DEBUG_START` / `# DEBUG_END` (Python)
or `# DEBUG_START` / `# DEBUG_END` (shell). Remove everything between and
including those markers.

## bin/radical-edge-wrapper.sh.in
- Lines 6–26: `_DBG`, `_dbg()` function and initial env dump
- Lines 31–36: post-export path/file existence checks and `--- exec starting ---`

## bin/radical-edge-service.py
- Lines 13–30: `import datetime`, `_dbg()` definition, startup env dump
- Lines ~71–74: `_dbg` calls around `validate_ssl_cert`
- Lines ~80–82: `_dbg` after EdgeService creation
- Lines ~91–93: `_dbg` before `service.run()`
- Lines ~96–98: `_dbg` in CancelledError handler
- Lines ~101–103: `_dbg` in Exception handler
- Lines ~107–109: `_dbg` in finally block

## src/radical/edge/service.py
- Lines ~3: `import datetime as _dt`
- Lines ~16–22: `_dbg()` definition
- Lines after `_load_plugins()`: `_dbg` in `__init__`
- Lines in `run()`: ws connect attempt log, ws connected log, register sent log
- Lines in exception handlers: connection lost log, unexpected error log

## src/radical/edge/plugin_psij.py
- Lines ~6: `import datetime as _dt`
- Lines ~22–28: `_dbg()` definition
- Lines in `submit_job`: spec dump, job id/paths/env dump, native_id after submit, exception log
- Lines in `_on_status`: state change log
