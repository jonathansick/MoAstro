twomass --- 2MASS Catalogs in MongoDB
=====================================

Setting up a local 2MASS Point Source Catalog
---------------------------------------------

The `twomass.PSC` class has methods for ingesting the 2MASS Point Source Catalog (PSC) for the whole sky into MongoDB. To get the raw data, FTP anonymously to ftp://ftp.ipac.caltech.edu/pub/2mass/allsky. Download all the `PSC_*.gz` files into a directory on your MongoDB server computer. Call this directory the `datadir/`. **Do not decompress the files.**

.. caution:: Each `PSC_*.gz` file is roughly 470 MB in size; the total download is 42.7 GB.

To load these PSC files into MongoDB we can use the :func:`twomass.import_compressed_psc` function. Boot up a Python interpreter, and run:

>>> import moastro.twomass
>>> moastro.twomass.import_compressed_psc(dataDir)

where `dataDir` is a string containing the path to your PSC download folder.

Note that :func:`twomass.import_compressed_psc()` has several keyword arguments to specify the host, and database/collection names (see API below).

The import will take some time. Once all the data is imported, an index is automatically built using the :meth:`twomass.PSC.index_space_color` class method. The indexing run in the background, and is thus non-blocking for other services using your MongoDB server.

Point Source Catalog queries
----------------------------

The PSC can be queried using the :meth:`twomass.PSC.find` method, which returns a :class:`pymongo.Cursor` instance. The only required argument of :meth:`twomass.PSC.find` is a dictionary specifying the MongoDB query. It may be an empty dictionary `{}` to return the *entire* PSC database.

In the PSC, each star is a *document*, and keys in that document correspond to columns in the 2MASS PSC schema (available at ftp://ftp.ipac.caltech.edu/pub/2mass/allsky/format_psc.html). In fact, we use the same names as in the 2MASS schema. Some useful fields include:

* `j_m`, `h_m` and `k_m` -- the *J*, *H* and *Ks* magnitudes respectively
* `j_msigcom`, `h_msigcom` and `k_msigcom` -- total photometric uncertainties in the *J*, *H* and *Ks* bands, respectively
* `designation` -- the official 2MASS designation of the star

Spatial queries
^^^^^^^^^^^^^^^
The only exception to the data naming convention is the RA-Dec coordinate. To allow spatial queries, the `ra` and `dec` columns are combined into an `(ra,dec)` tuple that is stored in the `coord` field.

The :meth:`twomass.PSC.find` method provides user-friendly facilities for building spatial queries. To get stars within the field of a certain FITS image, the user can provide either a pyfits header object, or a pywcs.WCS instance. A polygon query in the image footprint is automatically built. Boxes (spans or RA and Dec) can also be queried on, along with circles around points. See the :meth:`twomass.PSC.find` documentation for more details.

Note that these spatial queries *augment* any other query parameters that the user passed to the :meth:`PSC.find` method.

Limiting the fields returned
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The PSC has a number of fields for each star that may not be relevant in all cases. In fact, the performance of your query is decreased by returning superflous fields. To limit the fields that are returned in the cursor, you can pass a list of field names with the `fields` argument.

PSC API
-------

.. autoclass:: twomass.PSC
   :members:

Setup functions API
-------------------

.. autofunction:: twomass.import_compressed_psc
.. autofunction:: twomass.test_import_psc
