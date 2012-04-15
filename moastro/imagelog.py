import glob
import os
import shutil
import pymongo
import pyfits
import subprocess
import multiprocessing

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
    def __init__(self, url="localhost", port=27017):
        super(ImageLog, self).__init__()
        connection = pymongo.Connection(url, port)
        self.db = connection.m31
        self.collection = self.db.images # collection for all images from a camera: WIRCam/MegaCam, etc.
        self.queryMask = {}
        self.exts = ["0"]
        self.url = url
        self.port = port
    
    def __getitem__(self, key):
        """:return: a document (`dict` type) for the image named `key`"""
        selector = {"_id": key}
        selector.update(self.queryMask)
        return self.collection.find_one(selector)
    
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
        """Updates an image record by setting the `key` field to the given `value`."""
        if ext == None:
            self.collection.update({"_id": imageKey}, {"$set": {key: value}})
        else:
            self.collection.update({"_id": imageKey}, {"$set": {".".join((str(ext),key)): value}})
    
    def set_frames(self, key, data):
        """Does an update of data into the `key` field for data of an arbitrary
        collection of detectors.
        
        :param data: a dictionary of `frame: datum`, where `frame` is a tuple
            of (imageKey, ext)
        """
        for (imageKey, ext), datum in data:
            self.set(imageKey, key, datum, ext=ext)
    
    def search(self, selector, candidateImages=None):
        """Get images keys that match the specified selector using MongoDB queries.
        
        :param selector: dictionary for data keys: data values that specifies
            what image keys should be returned. Any mongodb search dictionary
            will work.
        :param candidateImages: (optional) a list of images to draw from; only images
            within the candidateImages set will be considered
        :return: the list of image keys for images that match the selector
        dictionary."""
        selector = self._insert_query_mask(selector)
        if candidateImages is not None:
            candidateImages = [candidateImages]
            selector.update({"_id": {"$in": candidateImages}})
        records = self.collection.find(selector, {"_id":1})
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
        """
        selector = self._insert_query_mask(selector)
        print "getiter using selector", selector
        if candidateImages is not None:
            selector.update({"_id": {"$in": candidateImages}})
        if type(dataKeys) == str:
            dataKeys = [dataKeys]
        return self.collection.find(selector, fields=dataKeys)
    
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
        """
        records = {}
        for record in self.getiter(selector, dataKeys, candidateImages=candidateImages):
            record = dict(record)
            imageKey = record['_id']
            records[imageKey] = record
        return records
    
    def get_images(self, imageKeys, dataKeys):
        """Same as `get()`, but operates on a sequence of image keys, rather
        than a search selector.
        """
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
        """
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
    
    def decompress_fits(self, pathKey, decompDir=None,
        decompPathKey=None, selector={}, delete=False, overwrite=False,
        nthreads=multiprocessing.cpu_count()):
        """Decompresses FITS files at `pathKey`.
        
        :param pathKey: field where FITS paths are found
        :param decompPathKey: (optional) can be set to a field where the
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
        
        if decompPathKey is None:
            decmpPathKey = pathKey
        
        records = self.getiter(selector, pathKey)
        args = []
        for rec in records:
            origPath = rec[pathKey]
            if decompDir is not None:
                outputPath = os.path.join(decompDir,
                    os.path.basename(os.path.splitext(origPath)[0]))
            else:
                outputPath = os.path.splitext(origPath)[0]
            
            # verify that this file exists, and possibly skip is
            exists = False
            if os.path.exists(outputPath):
                exists = True
                if overwrite: os.remove(outputPath)
            if exists:
                if rec.has_key(decompPathKey):
                    if rec[decompPathKey] == outputPath:
                        continue # this file is already decomp and recorded
                
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
            self.set(imageKey, decompPathKey, outputPath)
    
    def rename_field(self, dataKeyOld, dataKeyNew, selector=None):
        """Renames a field for all image log records found with the optional
        selector.
        
        :param dataKeyOld: original field name
        :param dataKeyNew: new field name
        :param selector: (optional) search selector dictionary
        """
        if selector is None:
            selector = {}
        records = self.get(selector, dataKeyOld, candidateImages=None)
        for imageKey, rec in records:
            self.collection.update({"_id": imageKey}, {"$unset": dataKeyOld})
            self.collection.update({"_id": imageKey}, {"$set":
                {dataKeyNew: rec[dataKeyOld]}})
    
    def delete_field(self, dataKey, selector=None):
        """Deletes a field from selected image records.
        
        :param dataKey: field to be deleted.
        :param selector: (optional) search selector dictionary
        """
        if selector is None:
            selector = {}
        selector = self._insert_query_mask(selector)
        self.collection.update(selector, {"$unset": {dataKey: 1}})
    
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
                
            self.collection.update({"_id": imageKey}, {"$set": {pathKey: newPath}})
    
    def delete_files(self, pathKey, selector=None):
        """Deletes all files stored under pathKey, and the image log fields"""
        if selector is None:
            selector = {}
        selector = self._insert_query_mask(selector)
        for rec in self.getiter(selector, pathKey):
            imageKey = rec['_id']
            if "." in pathKey:
                ext, k = pathKey.split(".")
                path = rec[ext][k]
            else:
                path = rec[pathKey]
            os.remove(path)
            print "Delete", imageKey, path
            self.collection.update({"_id": imageKey}, {"$unset": pathKey})
            
    
    def print_rec(self, imageKey):
        """Pretty-prints the record of `imageKey`"""
        selector = {"_id": imageKey}
        selector.update(self.queryMask)
        record = self.collection.find_one(selector)
        keys = record.keys()
        keys.sort()
        print "== %s ==" % imageKey
        for key in keys:
            if str(key) in self.exts: continue
            print "%s:" % key,
            print record[key]

def _funpack_worker(args):
    """Worker function for funpacking."""
    imageKey, command, outputPath = args
    subprocess.call(command, shell=True)
    return imageKey, outputPath
