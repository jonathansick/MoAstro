***************************************
auth --- Access secured MongoDB servers
***************************************

The `auth` module makes connecting to MongoDB instances easier. The same Python code can decide to connect to a MongoDB server locally, or over an SSH tunnel, depending on where the code is called from. This is handled with a ~/.moastro_auth file on each client computer.

Background
==========

Normally, connecting to a MongoDB database requires several arguments:

1. host url,
2. port, and
3. database name.

If the database is secured, a

4. username, and
5. password

are also required. If one is always running a code locally, this is not a problem. However, in collaborative environments, the host will need to be changed programmatically from `localhost` to the external URL of the database server. The `moastro.auth` module provides a standard interface for connecting to a MongoDB database from a user's perspective.

The `moasto` credentials format
===============================

Python codes can create a pymongo connection to a database by specifying a named database.

Credentials for MongoDB databases are written in `~/.moastro_auth` in JSON format. For example:
    
.. code-block:: none
   
   {"db_key": {"db_name": the_db_name,
               "host": host_url,
               "port": port_on_host}
   }

Each database that your application connects to would be an extra key-value in the `.moastro_auth` file.

Requesting a SSH tunnel
-----------------------

How to specify an SSH tunnel in `~/.moastro_auth`.

Implementation Notes
====================

SSH
---

Use paramiko? http://www.lag.net/paramiko e.g. This example is exactly what I'd want to do: https://github.com/robey/paramiko/blob/master/demos/forward.py

See also 

* http://jessenoller.com/2009/02/05/ssh-programming-with-paramiko-completely-different/
* http://stackoverflow.com/questions/2777884/shutting-down-ssh-tunnel-in-paramiko-programatically (set up a separate thread to hold the tunnel; shut down
  when no longer needed)

Object Design
-------------

Use the Borg Pattern (Martelli 2005; 6.16) to ensure that all Credentials classes share the same state and obviate needing to re-read the auth file. Also, the Credentials class should be split from the Connector class. The Connector class would actually set up the SSH tunnels, and cache db objects, etc. Credentials
would simply parse the credentials file and provide access.

API
===

.. autoclass:: auth.Credentials
   :members:

