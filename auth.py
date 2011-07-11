#!/usr/bin/env python
# encoding: utf-8
"""
Handles authorizations for secured MongoDB connections.

The `Credentials` class can be used by any class to supply a username
and password to for moastro modules that are connected to a MongoDB
database that is secured.

History
-------
2011-07-11 - Created by Jonathan Sick

"""
import json

__all__ = ['']

class Credentials(object):
    """Stores credentials for MongoDB databases and facilitates DB connections

    Credentials for MongoDB databases are written in `~/.moastro_auth` in
    JSON format. For example:

    {"localhost":
        {"my_db": {
            "user": "jsick",
            "pwd": "mypwd"},
         "my_other_db": {
            "user": "jsick",
            "pwd": "mypwd"},
        }
    }

    """
    def __init__(self):
        self._credentials = self._load_credentials()

    def _load_credentials(self):
        """Reads credentials from `~/.moastro_auth`."""
        f = open(".moastro.auth", 'r')
        credData = json.loads(f)
        f.close()

        self.credentials = {}
        for host in credData.iteritems():
            self.credentials[host] = {}
            for dbName, v in credData[host].iteritems():
                if 'user' not in v: continue
                if 'pwd' not in v: continue
                if type(v['user']) is not unicode: continue
                if type(v['pwd']) is not unicode: continue
                self.credentials[host][dbName] = {'user': v['user'],
                        'pwd': v['pwd']}

    def connect_db(self, dbname, connection):
        """Authorizes a MongoDB database."""
        pass


if __name__ == '__main__':
    main()


