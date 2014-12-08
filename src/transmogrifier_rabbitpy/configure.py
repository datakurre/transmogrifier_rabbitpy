# -*- coding: utf-8 -*-
"""
This module scans all direct submodules for component registrations in this
package when included for zope.configuration with venusianconfiguration.

"""
import importlib
import os
import pkg_resources
from io import StringIO

from configparser import RawConfigParser
from venusianconfiguration import configure
from venusianconfiguration import scan


# Scan modules
for resource in pkg_resources.resource_listdir(__package__, ''):
    name, ext = os.path.splitext(resource)
    if ext == '.py' and name not in ('__init__', 'configure'):
        path = '{0:s}.{1:s}'.format(__package__, name)
        scan(importlib.import_module(path))


# Register pipelines
for resource in pkg_resources.resource_listdir(__package__, ''):
    name, ext = os.path.splitext(resource)

    if ext == '.cfg':
        # Parse to read title and description
        data = pkg_resources.resource_string(__package__, resource)
        config = RawConfigParser()
        config.readfp(StringIO(data.decode('utf-8')))

        # Register
        configure.transmogrifier.pipeline(
            name=name,
            title=config.get('transmogrifier', 'title'),
            description=config.get('transmogrifier', 'description'),
            configuration=resource
        )
