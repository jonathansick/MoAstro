#!/usr/bin/env python
# encoding: utf-8
"""
Utility classes/functions for using MongoDB

2012-05-03 - Created by Jonathan Sick
"""


def reach(doc, key):
    """Returns a value from an embedded document.
    
    >>> embeddedDict = {"first": {"second": "gold!"}}
    >>> reach(embeddedDict, "first.second")
    gold!

    Parameters
    ----------
    doc : dict compatible
        Document returned by a MongoDB cursor
    key : str
        Key pointing to value being sought. Can use the MongoDB dot notation
        to refer to values in embedded documents. Root level keys (without
        dots) are perfectly safe with this function.
    """
    parts = key.split(".")
    if len(parts) > 1:
        return reach(doc[parts[0]], ".".join(parts[1:]))
    else:
        return doc[parts[0]]

if __name__ == '__main__':
    embeddedDict = {"first": {"second": "gold!"}}
    print reach(embeddedDict, "first.second")
    print reach(embeddedDict, "first")
