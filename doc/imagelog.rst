imagelog -- Image querying and metadata management
==================================================

In an image log, each image is given a unique key, and MongoDB document.
Images can be queried and their metadata returned.

Mo'Astro's image log also includes built-in facilities for moving and
deleting files, and compress/decompressing FITS files using FPACK (not
included).

It is the user's responsibilty to subclass image log and to provide methods
for ingesting data into image log. Contact jsick at astro dot queensu dot ca for subclass ideas.

Methods for Querying the Image Log
----------------------------------

:class:`ImageLog` uses four principle methods for querying the image log: :meth:`find`, :meth:`find_dict`, :meth:`find_images` and :meth:`distinct`.

.. automethod:: imagelog.ImageLog.find
.. automethod:: imagelog.ImageLog.find_dict
.. automethod:: imagelog.ImageLog.find_images
.. automethod:: imagelog.ImageLog.distinct
   

ImageLog API
------------

.. autoclass:: imagelog.ImageLog
   :members:
