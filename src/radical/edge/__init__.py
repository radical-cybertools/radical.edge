import os

from .plugin_lucid      import PluginLucid  # noqa: F401
from .plugin_xgfabric   import PluginXGFabric  # noqa: F401
from .plugin_queue_info import PluginQueueInfo  # noqa: F401
from .plugin_sysinfo    import PluginSysInfo  # noqa: F401
from .plugin_psij       import PluginPSIJ  # noqa: F401

from .plugin_base           import Plugin  # noqa: F401
from .plugin_session_base   import PluginSession  # noqa: F401
from .plugin_session_managed import SessionManagedPlugin  # noqa: F401

from .service           import EdgeService  # noqa: F401
from .client            import BridgeClient, EdgeClient, PluginClient  # noqa: F401


# Read version from VERSION file
_mod_root = os.path.dirname(__file__)
# VERSION is at the repo root, which is 3 levels up from src/radical/edge ?
# But __file__ is .../src/radical/edge/__init__.py
# So VERSION is at .../VERSION. 
# Relative path: ../../../VERSION
_repo_root = os.path.abspath(os.path.join(_mod_root, '..', '..', '..'))
_version_path = os.path.join(_repo_root, 'VERSION')

if not os.path.exists(_version_path):
    # Try local dir (if installed package included it)
    _version_path = os.path.join(_mod_root, 'VERSION')

try:
    with open(_version_path, 'r') as f:
        version = f.readline().strip()
        __version__ = version
except FileNotFoundError:
    version = 'unknown'
    __version__ = 'unknown'


