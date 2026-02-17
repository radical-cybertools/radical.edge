#!/usr/bin/env python

import radical.edge

try:
    print("version: %s" % radical.edge.version)
except Exception as e:
    print("version: unknown (%s)" % e)

# detail not available in __init__.py
# print("details: %s" % radical.edge.version_detail)

