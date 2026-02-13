import os

from .plugin_lucid      import PluginLucid
from .plugin_xgfabric   import PluginXGFabric
from .plugin_queue_info import PluginQueueInfo


# ------------------------------------------------------------------------------
# Read version from VERSION file
_mod_root = os.path.dirname(__file__)
_version_path = os.path.join(_mod_root, 'VERSION')

try:
    with open(_version_path, 'r') as f:
        version = f.readline().strip()
        __version__ = version
except FileNotFoundError:
    version = 'unknown'
    __version__ = 'unknown'


# ------------------------------------------------------------------------------
