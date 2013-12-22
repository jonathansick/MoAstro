#!/usr/bin/env python
# encoding: utf-8
"""
Helpers for loading package data.
"""

import os


def _get_data_dir():
    here = os.path.dirname(__file__)
    datadir = os.path.abspath(os.path.normpath(os.path.join(here, "../data")))
    assert os.path.exists(datadir)
    return datadir


def data_path(name):
    """Get the *path* to the package data.
    
    Parameters
    ----------

    name : str
        Name of the data file, relative to the data directory root.

    Returns
    -------

    path : str
        Absolute path to the data file.
    """
    return os.path.join(_get_data_dir(), name)
