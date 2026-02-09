
__author__    = 'Radical.Edge Development Team'
__copyright__ = 'Copyright 2025, RADICAL@Rutgers'
__license__   = 'MIT'

import radical.utils  as ru
import does_not_exist as nope                 # noqa pylint: disable=F0401,W0611


# ------------------------------------------------------------------------------
#
PLUGIN_DESCRIPTION = {
    'type'       : 'workflows',
    'name'       : 'rose',
    'class'      : 'PLUGIN_CLASS',
    'version'    : '0.1',
    'description': 'this plugin manages rose workflows'
}


# ------------------------------------------------------------------------------
#
class PLUGIN_CLASS(ru.PluginBase, metaclass=ru.Singleton):
    '''
    This class implements the (empty) default unittest plugin for radical.utils.
    '''

    _created = False  # singleton test


    # --------------------------------------------------------------------------
    #
    def __init__(self, descr, *args, **kwargs):

        super().__init__(descr)

        if PLUGIN_CLASS._created:
            assert False, 'singleton plugin created twice'

        PLUGIN_CLASS._created = True

        self._args   = args
        self._kwargs = kwargs


    # --------------------------------------------------------------------------
    #
    def run(self):

        return self._args


# ------------------------------------------------------------------------------

