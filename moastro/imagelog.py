import glob
import os
import shutil
import pymongo
import pyfits
import subprocess
import multiprocessing
import warnings

from dbtools import DotReachable


class ImageLog(object):
    """Base class for all mongodb-based image logs.
    
    The intent is for this base class to access the entire collection of image
    records for a given project. Subclasses can be used that cater to specific
    parts of the data set (such as one instrument or another; one observing
    mode or another). This specificity is created with the `queryMask`, which is
    a query that will always be appending to the user's query to ensure
    that only the requested types of data are returned. For example, this can
    be a query to only accept 'MegaPrime' under the `INSTRUME` key.
    """
    def __init__(self, dbname, cname, url="localhost", port=27017):
        super(ImageLog, self).__init__()
        connection = pymongo.Connection(url, port)
        self.db = connection[dbname]
        self.db.add_son_manipulator(DotReachable())
        self.c = self.db[cname]
        self.dbname = dbname
        self.cname = cname
        self.url = url
        self.port = port
        self.queryMask = {}
        self.exts = ["0"]
    
    def __getitem__(self, key):
        """:return: a document (`dict` type) for the image named `key`"""
        selector = {"_id": key}
        selector.update(self.queryMask)
        return self.c.find_one(selector)
    
    def _insert_query_mask(self, selector):
        """Enforces the query mask on the selector. The user can still override
        the query mask.
        """
        selector = dict(selector)
        for k, v in self.queryMask.iteritems():
            if k in selector: continue
            selector[k] = v
        return selector
    
    def set(self, imageKey, key, value, ext=None):
        """Updates an image record by setting the `key` field to the given
        `value`.
        """
        if ext == None:
            self.c.update({"_id": imageKey}, {"$set": {key: value}})
        else:
            self.c.update({"_id": imageKey},
                    {"$set": {".".join((str(ext), key)): value}})
    
    def set_frames(self, key, data):
        """Does an update of data into the `key` field for data of an arbitrary
        collection of detectors.
        
        :param data: a dictionary of `frame: datum`, where `frame` is a tuple
            of (imageKey, ext)
        """
        for (imageKey, ext), datum in data:
            self.set(imageKey, key, datum, ext=ext)

    def find(self, selector, images=None, one=False, **mdbArgs):
        """Wrapper around MongoDB `find()`."""
        selector = self._insert_query_mask(selector)
        if images is not None:
            selector.update({"_id": {"$in": images}})
        if one:
            return self.c.find_one(selector, **mdbArgs)
        else:
            return self.c.find(selector, **mdbArgs)

    def find_dict(self, selector, images=None, fields=None):
        """Analogous to find(), but formats the returned cursor
        into a dictionary."""
        c = self.find(selector, images=images, fields=fields, one=False)
        records = {}
        for doc in c:
            doc = dict(doc)
            imageKey = doc['_id']
            records[imageKey] = doc
        return records

    def find_images(self, selector, images=None):
        """Get images keys that match the specified selector using MongoDB
        queries.
        """
        selector = self._insert_query_mask(selector)
        if images is not None:
            selector.update({"_id": {"$in": images}})
        records = self.c.find(selector, {"_id": 1})
        imageKeys = [rec['_id'] for rec in records]
        imageKeys.sort()
        return imageKeys

    def distinct(self, field, selector, images=None):
        """Return the set of distinct values a field takes over the
        selection.
        """
        cursor = self.find(selector, images=images, fields=[field])
        return cursor.distinct(field)

    def compress_fits(self, pathKey, selector={}, candidateImages=None,
        alg="Rice", q=4, delete=False):
        """:param alg: Compression algorithm. Any of:
        * Rice
        * gzip
        :param q: quantization for floating-point images.
        :param delete: set to true if the un-compressed original should be
            deleted
        """
        algs = {"Rice": "-r", "gzip": "-g"}
        
        records = self.getiter(selector, pathKey, candidateImages=candidateImages)
        
        optList = []
        optList.append(algs[alg])
        optList.append(str(q))
        if delete == True:
            optList.append("-D")
        options = " ".join(optList)
        
        for rec in records:
            origPath = rec[pathKey]
            # Compress with fpack
            subprocess.call("fpack %s %s" % (options, origPath), shell=True)
            # Update filename with .fz extension
            outputPath = origPath + ".fz"
            self.set(rec['_id'], pathKey, outputPath)
    
    def decompress_fits(self, pathKey, decompKey=None,
            decompDir=None, selector={}, delete=False, overwrite=False,
            nthreads=multiprocessing.cpu_count()):
        """Decompresses FITS files at `pathKey`.
        
        :param pathKey: field where FITS paths are found
        :param decompKey: (optional) can be set to a field where the
            decompressed file can be found. Otherwise, the decompressed
            file path is written to `pathKey`.
        :param selector: (optional) search criteria dictionary
        :param delete: set to True to delete the compressed file.
        :param overwrite: if False, then funpack will skip any files
            that have already been decompressed. That is, the output file must
            be present and recorded in the image log under decompPathKey.
        :param nthreads: set to the number of threads. Multiprocessing is
            used if ``nthreads`` > 1.
        """
        if decompDir is not None:
            if os.path.exists(decompDir) is False:
                os.makedirs(decompDir)
        
        if decompKey is None:
            decompKey = pathKey
        
        records = self.getiter(selector, pathKey)
        args = []
        for rec in records:
            origPath = rec[pathKey]
            if decompDir is not None:
                outputPath = os.path.join(decompDir,
                    os.path.basename(os.path.splitext(origPath)[0]))
            else:
                outputPath = os.path.splitext(origPath)[0]
            
            # verify that this file exists, and possibly skip it
            exists = False
            if os.path.exists(outputPath):
                exists = True
                if overwrite: os.remove(outputPath)
            if exists:
                if decompKey in rec:
                    if rec[decompKey] == outputPath:
                        continue  # this file is already decomp and recorded
                
            options = ["-O %s" % outputPath]
            if delete is True:
                options.append("-D")
            
            command = "funpack %s %s" % (" ".join(options), origPath)
            print command
            
            args.append((rec['_id'], command, outputPath))
        
        if nthreads > 1:
            pool = multiprocessing.Pool(processes=nthreads)
            results = pool.map(_funpack_worker, args)
        else:
            results = map(_funpack_worker, args)
        
        for result in results:
            imageKey, outputPath = result
            self.set(imageKey, decompKey, outputPath)
    
    def rename_field(self, dataKeyOld, dataKeyNew, selector=None, multi=True):
        """Renames a field for all image log records found with the optional
        selector.
        
        :param dataKeyOld: original field name
        :param dataKeyNew: new field name
        :param selector: (optional) search selector dictionary
        """
        if selector is None:
            selector = {}
        selector = self._insert_query_mask(selector)
        selector.update({dataKeyOld: {"$exists": 1}})
        ret = self.c.update(selector,
                {"$rename": {dataKeyOld: dataKeyNew}},
                multi=multi, safe=True)
        print ret

    def delete_field(self, dataKey, selector=None, multi=True):
        """Deletes a field from selected image records.
        
        :param dataKey: field to be deleted.
        :param selector: (optional) search selector dictionary
        """
        if selector is None:
            selector = {}
        selector = self._insert_query_mask(selector)
        print "using multi delete", multi
        self.c.update(selector, {"$unset": {dataKey: 1}},
                multi=multi)
    
    def move_files(self, pathKey, newDir, selector=None, copy=False):
        """Moves a file whose path is found under `pathKey` to the `newDir`
        directory. The directory is created if necessary. Old files are
        overwritten if necessary.
        
        :param dataKey: field of path for files to be moved
        :param newDir: directory where files should be moved to.
        :param selector: (optional) search selector dictionary
        """
        if os.path.exists(newDir) is False:
            os.makedirs(newDir)
        
        if selector is None:
            selector = {}
        selector = self._insert_query_mask(selector)
        for rec in self.getiter(selector, pathKey):
            imageKey = rec['_id']
            origPath = rec[pathKey]
            newPath = os.path.join(newDir, os.path.basename(origPath))
            if newPath == origPath:
                continue
            print origPath, "->", newPath
            # if os.path.exists(newPath):
            #     os.remove(newPath)
            
            if copy:
                shutil.copy(origPath, newPath)
            else:
                shutil.move(origPath, newPath)
                
            self.c.update({"_id": imageKey}, {"$set": {pathKey: newPath}})
    
    def delete_files(self, pathKey, selector=None):
        """Deletes all files stored under pathKey, and the image log fields"""
        if selector is None:
            selector = {}
        selector = self._insert_query_mask(selector)
        for rec in self.getiter(selector, pathKey):
            imageKey = rec['_id']
            path = rec[pathKey]
            if os.path.exists(path):
                os.remove(path)
            print "Delete", imageKey, path
            self.c.update({"_id": imageKey}, {"$unset": pathKey})
    
    def print_rec(self, imageKey):
        """Pretty-prints the record of `imageKey`"""
        selector = {"_id": imageKey}
        selector.update(self.queryMask)
        record = self.c.find_one(selector)
        keys = record.keys()
        keys.sort()
        print "== %s ==" % imageKey
        for key in keys:
            if str(key) in self.exts: continue
            print "%s:" % key,
            print record[key]

    def search(self, selector, candidateImages=None):
        """Get images keys that match the specified selector using MongoDB queries.
        
        :param selector: dictionary for data keys: data values that specifies
            what image keys should be returned. Any mongodb search dictionary
            will work.
        :param candidateImages: (optional) a list of images to draw from; only images
            within the candidateImages set will be considered
        :return: the list of image keys for images that match the selector
        dictionary.
        
        .. deprecated::
           Use :meth:`find_images` instead.
        """
        warnings.warn(
            'search() is deprecated, use find_images() instead',
            stacklevel=2)

        selector = self._insert_query_mask(selector)
        if candidateImages is not None:
            candidateImages = [candidateImages]
            selector.update({"_id": {"$in": candidateImages}})
        records = self.c.find(selector, {"_id": 1})
        imageKeys = [rec['_id'] for rec in records]
        imageKeys.sort()
        return imageKeys
    
    def getiter(self, selector, dataKeys, candidateImages=None):
        """Returns a cursor to iterate through image records that meet the
        given selector. Each record is a dictionary, with the image key
        stored under `_id`.
        
        :param selector: dictionary for data keys: data values that specifies
            what image keys should be returned. Any mongodb search dictionary
            will work.
        :param dataKeys: data key(s) for each image that should be returned for
            the selected images. Can be string or a sequence of strings.
        :param candidateImages: (optional) a **list** of images to draw from;
            only images within the candidateImages set will be considered

        .. deprecated::
           Use :meth:`find` instead.
        """
        warnings.warn(
            'getiter() is deprecated, use find() instead',
            stacklevel=2)
        selector = self._insert_query_mask(selector)
        print "getiter using selector", selector
        if candidateImages is not None:
            selector.update({"_id": {"$in": candidateImages}})
        if type(dataKeys) == str:
            dataKeys = [dataKeys]
        return self.c.find(selector, fields=dataKeys)
    
    def get(self, selector, dataKeys, candidateImages=None):
        """Get a dictionary of `image key: {data key: data value, ...}` for images
        that match the search `selector`. The returned dictionary contains
        data only with the requested dataKeys (i.e., a subset of the data base
        and records for each image are returned.)
        
        :param selector: dictionary for data keys: data values that specifies
            what image keys should be returned. Any mongodb search dictionary
            will work.
        :param dataKeys: data for each image that should be returned for the
            selected images.
        :param candidateImages: (optional) a **list** of images to draw from;
            only images within the candidateImages set will be considered

        .. deprecated::
           Use :meth:`find_dict` instead.
        """
        warnings.warn(
            'get() is deprecated, use find_dict() instead', 
            stacklevel=2)
        records = {}
        for record in self.getiter(selector, dataKeys, candidateImages=candidateImages):
            record = dict(record)
            imageKey = record['_id']
            records[imageKey] = record
        return records
    
    def get_images(self, imageKeys, dataKeys):
        """Same as `get()`, but operates on a sequence of image keys, rather
        than a search selector.

        .. deprecated::
           Use :meth:`find_dict` instead.
        """
        warnings.warn(
            'get_images() is deprecated, use find_dict() instead', 
            stacklevel=2)
        return self.get({}, dataKeys, candidateImages=imageKeys)

    def find_unique(self, dataKey, selector={}, candidateImages=None):
        """Get the set of unique values of data key for images that meet the
        specified selector.
        
        .. note:: This doesn't actually use the `distinct` aggregation commmand
            in PyMongo, since it doesn't yet support queries itself.
        
        :param dataKey: data field whose values will compiled into a set of
            unique values.
        :param selector: (optional) dictionary for data keys: data values that
            specifies what image keys should be returned. Any mongodb search
            dictionary will work.
        :param candidateImages: (optional) a list of images to draw from; only
            images within the candidateImages set will be considered

        .. deprecated::
            Use :meth:`distinct` instead.
        """
        warnings.warn(
            'find_unique() is deprecated, use distinct() instead',
            stacklevel=2)
        records = self.getiter(selector, dataKey, candidateImages=candidateImages)
        itemList = [rec[dataKey] for rec in records]
        filteredItemList = []
        for item in itemList:
            if type(item) is unicode:
                filteredItemList.append(str(item))
            else:
                filteredItemList.append(item)
        print "all items:", filteredItemList
        valueSet = list(set(itemList))
        valueSet.sort()
        print "value set:", valueSet
        return valueSet


def _funpack_worker(args):
    """Worker function for funpacking."""
    imageKey, command, outputPath = args
    subprocess.call(command, shell=True)
    return imageKey, outputPath
