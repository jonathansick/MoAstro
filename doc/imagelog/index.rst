.. currentmodule:: imagelog

ImageLog -- Image querying and metadata management
==================================================

In an image log, each image is given a unique key, and MongoDB document.
Images can be queried and their metadata returned.

Mo'Astro's image log also includes built-in facilities for moving and
deleting files, and compress/decompressing FITS files using FPACK (not
included).

.. toctree::
   
   usage
   api

It is the user's responsibilty to subclass image log and to provide methods
for ingesting data into image log. Contact jsick at astro dot queensu dot ca for subclass ideas.
