#!/usr/bin/env python3

__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


"""
Test for version information.
"""

import radical.edge as re


# ------------------------------------------------------------------------------
def test_version_exists():
    """Test that version attribute exists."""
    assert hasattr(re, 'version')
    assert hasattr(re, '__version__')


# ------------------------------------------------------------------------------
def test_version_is_string():
    """Test that version is a string."""
    assert isinstance(re.version, str)
    assert isinstance(re.__version__, str)


# ------------------------------------------------------------------------------
def test_version_not_empty():
    """Test that version is not empty."""
    assert re.version
    assert re.__version__
    assert re.version != ''
    assert re.__version__ != ''


# ------------------------------------------------------------------------------
def test_version_matches():
    """Test that version and __version__ match."""
    assert re.version == re.__version__


# ------------------------------------------------------------------------------
def test_version_format():
    """Test that version has expected format (not 'unknown')."""
    # Should be either a version number or git hash, not 'unknown'
    assert re.version != 'unknown'
    assert len(re.version) > 0


# ------------------------------------------------------------------------------

