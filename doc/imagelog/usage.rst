.. currentmodule:: imagelog

Using ImageLog
==============

:class:`ImageLog`'s methods provide a convient facade against MongoDB, the filesystem, and even FITS compression utilities. This section lists the
methods in :class:`ImageLog` that can be used for query, updating, file management, and DB maintenance. Click on the method names to see full documentation in the API page.

Methods for Querying the Image Log
----------------------------------

:class:`ImageLog` uses four principle methods for querying the image log:

.. autosummary::
   :toctree:

   ImageLog.find
   ImageLog.find_dict
   ImageLog.find_images
   ImageLog.distinct

Methods for Setting Metadata
----------------------------

.. autosummary::
   :toctree:

   ImageLog.set
   ImageLog.set_frames

Methods for Working with Files
------------------------------

.. autosummary::
   :toctree:

   ImageLog.decompress_fits
   ImageLog.compress_fits
   ImageLog.move_files
   ImageLog.delete_files

Methods for Maintaining the ImageLog
------------------------------------

.. autosummary::
   :toctree:

   ImageLog.rename_field
   ImageLog.delete_field
