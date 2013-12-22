#!/usr/bin/env python
# encoding: utf-8
"""
Handle settings for Mo'Astro databases.

Settings are stored on disk as a JSON file located at ``$HOME/.moastro.json``.
Alternative locations can be set with the ``$MOASTROCONFIG`` environment
variable.

A basic ``.moastro.json`` file looks like this::

    {"servers":
        {"marvin": {"url": "localhost", "port": 27017}}
    }


Server definitions are stored as a hash under the ``servers`` key. Here we
define one server named ``marvin`` that is connected to as ``localhost:27017``.

In the future we will add ``remote_url`` and ``remote_port`` to the settings
schema to facilitate SSH port forwarding.


Functions
---------

- :func:`read_settings`
- :func:`locate_server`
"""

import os
import json
import logging


def read_settings(path=os.getenv('MOASTROCONFIG',
        os.path.expandvars('$HOME/.moastro.json'))):
    """Read the Mo'Astro JSON configurations file.
    
    Parameters
    ----------

    path : str
        Path to the ``.moastro.json`` file.

    Returns
    -------

    settings : dict
        The settings, as a ``dict``. If the settings file is not found,
        an empty dictionary is returned.
    """
    try:
        with open(path, 'r') as f:
            return json.loads(f.read())
    except IOError:
        log = logging.getLogger('moastro')
        log.warning("{path} config file not found".format(path=path))
        return {}
