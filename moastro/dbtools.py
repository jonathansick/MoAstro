#!/usr/bin/env python
# encoding: utf-8
"""
Utility classes/functions for using MongoDB

2012-05-03 - Created by Jonathan Sick
"""
from pymongo.son_manipulator import SONManipulator


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


class ReachableDoc(dict):
    def __init__(self, *args, **kwargs):
        super(ReachableDoc, self).__init__(*args, **kwargs)
    
    def __getitem__(self, key):
        """docstring for __getitem__"""
        if "." in key:
            parts = key.split(".")
            return reach(super(ReachableDoc, self).__getitem__(parts[0]),
                    ".".join(parts[1:]))
        else:
            return super(ReachableDoc, self).__getitem__(key)

    def what_am_i(self):
        return "I'm an instance of MyDoc!"


class DocManipulator(SONManipulator):
    def transform_outgoing(self, son, collection):
        return ReachableDoc(son)


def test_reachdoc():
    c = pymongo.Connection()
    db = c.foo
    db.add_son_manipulator(DocManipulator())
    db.bar.remove()
    db.bar.insert({'foo': {'bar': 'baz'}})
    doc = db.bar.find_one()
    assert isinstance(doc, ReachableDoc)
    print type(doc)
    pprint.pprint(doc)
    print doc.what_am_i()
    print type(doc['foo'])
    print doc['foo.bar']
    print doc['foo']


def test_reach():
    """docstring for test_reach"""
    embeddedDict = {"first": {"second": "gold!"}}
    print reach(embeddedDict, "first.second")
    print reach(embeddedDict, "first")

if __name__ == '__main__':
    import pprint
    import pymongo
    test_reachdoc()
    test_reach()
