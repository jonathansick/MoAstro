.. currentmodule:: moastro.imagelog

Using ImageLog
==============

:class:`ImageLog`'s methods provide a convient facade against MongoDB, the filesystem, and even FITS compression utilities. This section lists the
methods in :class:`ImageLog` that can be used for query, updating, file management, and DB maintenance. Click on the method names to see full documentation in the API page.

Methods for Querying the Image Log
----------------------------------

:class:`ImageLog` uses four principle methods for querying the image log:

- :meth:`ImageLog.find` to run general MongoDB queries and return a cursor.
- :meth:`ImageLog.find_dict` to get a dictionary instead.
- :meth:`ImageLog.find_images` to get image keys.
- :meth:`ImageLog.distinct` runs a ``distinct`` method call on the cursor.


Methods for Setting Metadata
----------------------------

- :meth:`ImageLog.set` to perform a update on a single document and field.
- :meth:`ImageLog.set_frames` to perform an update on an image extension field.


Methods for Working with Files
------------------------------

- :meth:`ImageLog.decompress_fits` to run funpack.
- :meth:`ImageLog.compress_fits` to run fpack.
- :meth:`ImageLog.move_files` to run move files.
- :meth:`ImageLog.delete_files` to run delete files from disk.


Methods for Maintaining the ImageLog
------------------------------------

- :meth:`ImageLog.rename_field` to change the name of a field in an image document.
- :meth:`ImageLog.delete_field` to delete a field from an image document.
