#!/usr/bin/env python
# encoding: utf-8
"""
Handles authorizations for secured MongoDB connections.

The `Credentials` class can be used by any class to supply a username
and password to for moastro modules that are connected to a MongoDB
database that is secured.

.. todo:: Think about supporting SSH tunnels instead of relying on MongoDB
   authorizations.
   
   e.g. ssh -L 27017:localhost:27017 user@myserver

   See http://groups.google.com/group/mongomapper/browse_thread/thread/80a11b06cf1f7711

.. todo:: Use the singleton design pattern for Credentials

.. todo:: Should Credentials be renamed to Connector, or spun-out of the
   connection process?

History
-------
2011-07-11 - Created by Jonathan Sick
"""
import json
import pymongo

__all__ = ['']

class Credentials(object):
    """Stores credentials for MongoDB databases and facilitates DB connections.

    Credentials for MongoDB databases are written in `~/.moastro_auth` in
    JSON format. For example:
    
    .. code-block:: none
       
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
        self.connections = {}
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

    def connect_db(self, dbname, c=None, host="localhost", port=27017):
        """Authorizes a MongoDB database, returning the DB instance.

        There are two means of connecting to a database:

        1. Pass a `pymongo.Connection` instance of the host as
            the `c` keyword argument
        2. Pass the host URL and port using the `host` and `port` keyword
            arguments.

        A `Connection` instance takes precedence over `host` and `port`.

        :param dbname: (required) the name of the database
        :param c: A pymongo `Connection` instance.
        :param host: Address (IP) of the MongoDB server
        :param port: Port that the MongoDB server uses

        :returns: `pymongo.database` instance.
        """
        if c is not None:
            host = c.host
            port = c.port
        else:
            # See if we stashed a connection
            if host in self.connections:
                c = self.connections[host]
            else:
                c = pymongo.Connection(host=host, port=port)
                self.connections[host] = c
        db = c[dbname]

        # Figure out if we need to authenticate
        if 'user' in self.credentials[host][dbname] and \
                'pwd' in self.credentials[host][dbname]:
            user = self.credentials[host][dbname]['user']
            password = self.credentials[host][dbname]['pwd']
            db.authenticate(user, password)

        return db


if __name__ == '__main__':
    main()


