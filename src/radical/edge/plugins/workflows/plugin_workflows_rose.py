
from abc import ABC

import does_not_exist as nope                 # noqa pylint: disable=F0401,W0611


# ------------------------------------------------------------------------------
#
# Simple Singleton metaclass implementation
class Singleton(type):
    """Metaclass that creates a Singleton base class when called."""
    _instances = {}
    
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


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
class PLUGIN_CLASS(ABC, metaclass=Singleton):
    '''
    This class implements the (empty) default unittest plugin.
    '''

    _created = False  # singleton test


    # --------------------------------------------------------------------------
    #
    def __init__(self, descr, *args, **kwargs):

        self._descr  = descr

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

