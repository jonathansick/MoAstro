"""Wrappers around the astromatic.net software library."""

import os
import shutil
import glob
import subprocess
import multiprocessing

try:
    from astropy.io import fits as pyfits
except ImportError:
    import pyfits

from imagelog import ImageLog
from dbtools import reach


class Astromatic(object):
    """Abstract base class for the Astromatic software wrappers
    (SExtractor, SCAMP, Swarp).
    """
    def __init__(self, configs=None, workDir=".", defaultsPath=None):
        self.configs = configs
        self.workDir = workDir
        self.defaultsPath = defaultsPath
        
        # Initialize the working directory
        if os.path.exists(self.workDir) is not True:
            print "making %s" % self.workDir
            os.makedirs(self.workDir)
    
    def add_default_param_to_configs(self, defaultsCommand,
            name="defaults.txt"):
        """Adds the default parameters filepath to the configs dictionary.
        Writes a new defaults file is one is not found/available.
        """
        # Write new defaults file if necessary
        if self.defaultsPath is None:
            self.write_defaults_file(defaultsCommand)
        elif os.path.exists(self.defaultsPath) is False:
            self.write_defaults_file(defaultsCommand)
        
        self.add_to_configs("c", self.defaultsPath)
    
    def write_defaults_file(self, defaultsCommand):
        """Writes the Swarp internal defaults and returns its path."""
        self.defaultsPath = os.path.join(self.workDir, "defaults.txt")
        print self.defaultsPath
        command = "%s > %s" % (defaultsCommand, self.defaultsPath)
        p = subprocess.Popen(command, shell=True)
        p.wait()
        
        return self.defaultsPath
    
    def write_input_file_list(self, inputFITSPaths, name="inputlist"):
        """Writes the path of each FITS file to a file
        *workDir*/inputlist.txt
        """
        listString = "\n".join(inputFITSPaths)  # one file path per line
        listString = "".join([listString, "\n"])  # append a newline at EOF
        
        listPath = os.path.join(self.workDir, ".".join((name, "txt")))
        if os.path.exists(listPath):
            os.remove(listPath)
        
        f = open(listPath, 'w')
        f.write(listString)
        f.close()
        
        return listPath
    
    def add_to_configs(self, key, value):
        """Add a key, with a value, to the configs dictionary. Use this to
        ensure the configs are initialized.
        """
        if self.configs is None:
            self.configs = {key: value}
        else:
            self.configs[key] = value
    
    def set_null_config(self, key, null="\"\""):
        """Sets the value of the key configuration to the null value, which
        is a null string ("") by default. If null is None, the the key is
        deleted altogether.
        """
        if self.configs is None:
            if null is not None:
                self.configs = {key: null}
            else:
                self.configs = {}
        else:
            if null is not None:
                self.configs[key] = null
            else:
                if key in self.configs:
                    del self.configs[key]
    
    def make_config_command(self):
        """Joins the command line configuration arguments together, returning
        a string.
        """
        if self.configs is not None:
            configCmd = ""
            for key, value in self.configs.iteritems():
                configCmd += " -%s %s" % (key, value)
            return configCmd
        else:
            return None
    
    def make_command(self):
        """Place holder subclass to generate the command line string for
        running the Terapix program.
        """
        pass
    
    def run(self, virtualHost=None):
        """Runs the command process.
        The optional virtualHost is a tuple with format:
            (user@host, rootPath)
        """
        # Make the terapix program command
        command = self.make_command()
        
        # Append the ssh call if we run on a virtual machine
        if virtualHost is not None:
            command = " ".join(["ssh",
                virtualHost[0],
                "'cd %s;%s'" % (virtualHost[1], command)])
        
        print command
        p = subprocess.Popen(command, shell=True)
        p.wait()


class Swarp(Astromatic):
    """This class wraps the functionality of Astromatic's Swarp mosaic
    software.
    """
    def __init__(self, imagePaths, mosaicName, scampHeadPaths=None,
            weightPaths=None, defaultsPath=None, configs=None,
            workDir="mosaic", uniqueExt=""):
        self.imagePaths = imagePaths
        self.uniqueExt = uniqueExt
        mosaicBasename = os.path.splitext(mosaicName)[0]
        self.mosaicPath = os.path.join(workDir, mosaicBasename + ".fits")
        self.mosaicWeightPath = os.path.join(workDir,
                mosaicBasename + "_weight.fits")
        
        if weightPaths is not None:
            self.useWeights = True
        else:
            self.useWeights = False
        
        self.swarpInputs = {}
        self.imageNames = [os.path.basename(os.path.splitext(imagePaths[i])[0])
                for i in xrange(len(self.imagePaths))]
        for i, key in enumerate(self.imageNames):
            print key
            if scampHeadPaths is None:
                headPath = None
            else:
                headPath = scampHeadPaths[i]
            
            if self.useWeights:
                weightPath = weightPaths[i]
            else:
                weightPath = None
            
            imagePath = imagePaths[i]
            self.swarpInputs[key] = {'head': headPath, 'path': imagePath,
                'weight': weightPath}
        
        super(Swarp, self).__init__(configs=configs, workDir=workDir,
            defaultsPath=defaultsPath)
    
    @classmethod
    def from_db(cls, imageLog, imageKeys, pathKey, mosaicName,
            scampHeadPathKey=None, weightPathKey=None, defaultsPath=None,
            configs=None, workDir="mosaic", uniqueExt=""):
        """Construct a Swarp run from database records."""
        dataKeys = [pathKey]
        if scampHeadPathKey is not None:
            dataKeys.append(scampHeadPathKey)
        if weightPathKey is not None:
            dataKeys.append(weightPathKey)
        
        recs = imageLog.find_dict({}, images=imageKeys, fields=dataKeys)
        imagePaths = [reach(recs[k], pathKey) for k in imageKeys]
        if scampHeadPathKey is not None:
            scampHeadPaths = [reach(recs[k], scampHeadPathKey)
                    for k in imageKeys]
        else:
            scampHeadPaths = None
        if weightPathKey is not None:
            weightPaths = [reach(recs[k], weightPathKey) for k in imageKeys]
        else:
            weightPaths = None
        
        return cls(imagePaths, mosaicName, scampHeadPaths=scampHeadPaths,
            weightPaths=weightPaths, defaultsPath=defaultsPath,
            configs=configs, workDir=workDir, uniqueExt="")
    
    def set_target_fits(self, targetFITSPath):
        """Use the header of `targetFITSPath` to define output pixel space.
        
        This method links `targetFITSPath` to *mosaicName*".head", which
        Swarp automatically recognizes.
        """
        targetFITSPath = os.path.abspath(targetFITSPath)
        destPath = os.path.splitext(self.mosaicPath)[0] + ".head"
        if os.path.lexists(destPath): os.remove(destPath)
        cmd = "ln -s %s %s" % (targetFITSPath, destPath)
        subprocess.call(cmd, shell=True)
    
    def set_target_header(self, header):
        """Alternative to `set_target_wcs`, it creates writes the header
        to the appropriate filename and tells swarp to use it as a base WCS
        reference.
        """
        headerText = str(header)
        self._write_target_header(headerText)
    
    def _write_target_header(self, headerText):
        """docstring for _write_target_header"""
        path = os.path.splitext(self.mosaicPath)[0] + ".head"
        if os.path.exists(path):
            os.remove(path)
        f = open(path, 'w')
        f.write(headerText)
        f.close()
    
    def make_command(self):
        """Produces the command to run Swarp."""
        # Add default parameters to the configs file. Writes a new defaults
        # if one is not found
        self.add_default_param_to_configs("swarp -d")
        
        # Write the input FITS file list to disk
        inputFITSPaths = []
        inputWeightPaths = []
        for key, db in self.swarpInputs.iteritems():
            inputFITSPaths.append(db['path'])
            inputWeightPaths.append(db['weight'])
        
        listPath = self.write_input_file_list(inputFITSPaths,
                name="inputlist" + self.uniqueExt)
        
        if self.useWeights:
            weightListPath = self.write_input_file_list(inputWeightPaths,
                    name="weightlist" + self.uniqueExt)
            self.add_to_configs("WEIGHT_IMAGE", "@" + weightListPath)
        else:
            # ensure there's no weight
            self.add_to_configs("WEIGHT_TYPE", "NONE")
            self.set_null_config("WEIGHT_IMAGE", null=None)

        # Setup external headers
        for key, db in self.swarpInputs.iteritems():
            imagePath = db['path']
            origHeaderPath = db['head']
            newHeaderPath = os.path.splitext(imagePath)[0] + ".head"
            if (origHeaderPath is not None) \
                    and (origHeaderPath != newHeaderPath):
                if os.path.exists(newHeaderPath):
                    os.remove(newHeaderPath)  # clean out old copies at dest.
                shutil.copy(origHeaderPath, newHeaderPath)

        # Make resampling directory if it does not exist
        if "RESAMPLE_DIR" in self.configs:
            self._resamp_dir = self.configs['RESAMPLE_DIR']
            if not os.path.exists(self._resamp_dir):
                os.makedirs(self._resamp_dir)
        else:
            self._resamp_dir = ""  # for resampled_paths() method
        
        # Form command with the inputlist
        command = "swarp @%s" % listPath
        
        # Append the output file configuration
        self.add_to_configs("IMAGEOUT_NAME", self.mosaicPath)
        self.add_to_configs("WEIGHTOUT_NAME", self.mosaicWeightPath)
        
        # Append all other configurations
        configCmd = self.make_config_command()
        if configCmd is not None:
            command = " ".join((command, configCmd))
        
        return command
    
    def mosaic_paths(self):
        """:return: tuple of (mosaic path, mosaic weight path)."""
        return self.mosaicPath, self.mosaicWeightPath

    def resampled_paths(self, exts, stringify_exts=True):
        """Return the paths to resampled images created by Swarp.
        
        Parameters
        ----------
        exts : sequence of `int`
            Integer indices of the MEF extension to get resampled images for.
            E.g., the WIRCam camera has 4 chips (and 4 FITS extensions) so
            ``exts=(1, 2, 3, 4)``.
        stringify_exts : bool
            If ``True``, the extension numbers in the returned dictionary will
            be converted to ``str`` to be compatible with JSON (for inserting
            into MongoDB).

        Returns
        -------
        resamp_paths : list
            Sequence of resampled path dictionaries for each input image, in
            the same order as paths were input. Each entry is a dictionary,
            keyed by the image extension number, whose value is the path
            to the resampled FITS image for this chip.
        resamp_wpaths : list
            Same as ``resamp_paths``, but for the weight images.
        """
        resamp_paths = []
        resamp_wpaths = []
        for source_path in self.imagePaths:
            src_root = os.path.splitext(os.path.basename(source_path))[0]
            im_paths = {}
            im_wpaths = {}
            for ext in exts:
                if ext == 0:
                    resamp_path = os.path.join(self._resamp_dir,
                            src_root + ".resamp.fits")
                    resamp_wpath = os.path.join(self._resamp_dir,
                            src_root + ".resamp.weight.fits")
                else:
                    resamp_path = os.path.join(self._resamp_dir,
                            src_root + ".%04i.resamp.fits" % ext)
                    resamp_wpath = os.path.join(self._resamp_dir,
                            src_root + ".%04i.resamp.weight.fits" % ext)
                if stringify_exts:
                    ext_id = str(ext)
                else:
                    ext_id = ext
                im_paths[ext_id] = resamp_path
                im_wpaths[ext_id] = resamp_wpath
            resamp_paths.append(im_paths)
            resamp_wpaths.append(im_wpaths)
        return resamp_paths, resamp_wpaths
    
    def write_input_file_list(self, paths, name="list"):
        """Override the Terapix class's method so that lists of imagePaths,
        associated with a single image image key can be expanded.
        
        The motivation for this is when doing a combine step on a previous
        resampling step. Resampling will break apart multi-extension FITS
        files, causing a single image of weight image to now be several
        images. When the user applies Swarp on the combine step, the
        imagePathKey (and the weightKey) must be lists to that set of FITS
        files.
        """
        newPathList = []
        for path in paths:
            if type(path) is list:
                newPathList += path
            else:
                newPathList.append(path)
        
        outputPath = super(Swarp, self).write_input_file_list(newPathList,
                name=name)
        return outputPath
    
    def copy_mosaic_wcs(self, headerPath):
        """Copies the WCS keys in the header of the mosaic image (as produced)
        by swarp.run() to an ascii header file at *headerPath*.
        
        This is useful for creating a .head file for use with a second run of
        Swarp with another data set (like a different colour) so that the
        pixel-sky projection is the same in both.
        
        TODO DEPRECATED Refactored as a function in Terapix.py. Keep this
        method around as a wrapper to Terapix.copyHeaderWCS().
        """
        mosaicFITS = pyfits.open(self.mosaicPath)
        mosaicHeader = mosaicFITS[0].header
        mosaicCardList = mosaicHeader.ascardlist()
        
        wcsKeys = ('EQUINOX', 'RADECSYS', 'CTYPE1', 'CUNIT1', 'CRVAL1',
                'CRPIX1', 'CD1_1', 'CD1_2', 'CTYPE2', 'CUNIT2', 'CRVAL2',
                'CRPIX2', 'CD2_1', 'CD2_2')
        
        # List of WCS header cards, as strings
        wcsCardList = []
        
        for key in wcsKeys:
            # try to find this key in the mosaic fits image
            try:
                # Trim the last character so that when we add a newline, it
                # is located at char#80
                cardStr = str(mosaicCardList[key])[:-1]
            except:
                continue  # evidently that key does not exist the mosaic FITS
            wcsCardList.append(cardStr)
        
        # Write the WCS cards, one per line, to headerPath
        headerText = "\n".join(wcsCardList)
        headerText = "".join((headerText, "\n"))  # append last newline
        if os.path.exists(headerPath):
            os.remove(headerPath)
        f = open(headerPath, 'w')
        f.write(headerText)
        f.close()
        
        mosaicFITS.close()


class Scamp(Astromatic):
    """Python wrapper class for Terapix's SCAMP application for astrometric
    and photometric registration of mosaics.
    
    .. note:: the useFileRefs=True feature seems to be broken b/c SCAMP doesn't
    recognize the files that it downloaded itself. For now, always use False
    for this option.
    """
    
    # Listing of all check plots available to scamp 1.4.2
    ALLPLOTS = ["SKY_ALL", "FGROUPS", "DISTORTION", "ASTR_INTERROR1D",
        "ASTR_INTERROR2D", "ASTR_REFERROR1D", "ASTR_REFERROR2D",
        "ASTR_PIXERROR1D", "ASTR_SUBPIXERROR1D", "ASTR_CHI2",
        "ASTR_REFSYSMAP", "ASTR_REFPROPER", "ASTR_COLSHIFT1D",
        "PHOT_ERROR", "PHOT_ERRORVSMAG", "PHOT_ZPCORR", "PHOT_ZPCORR3D",
        "SHEAR_VS_AIRMASS"]
    
    def __init__(self, seCatPaths, imageLog=None, imageKeys=None,
            defaultsPath=None, configs=None, checks=None, workDir="scamp",
            useFileRefs=False):
        self.catalogPaths = seCatPaths
        self.useFileRefs = useFileRefs
        self.imageLog = imageLog
        self.imageKeys = imageKeys
        self.headDB = {}
        for key, catPath in zip(self.imageKeys, self.catalogPaths):
            # Also, add path of .head file, to be produced by scamp, to a
            # database dictionary. The path is the FITS_LDAC's path, but
            # with a new extension.
            #
            # FIXME. how should this be done if no image keys are used?
            headPath = ".".join((os.path.splitext(catPath)[0], "head"))
            self.headDB[key] = headPath
        
        super(Scamp, self).__init__(configs=configs, workDir=workDir,
            defaultsPath=defaultsPath)
        
        # Initialize the check plots
        if checks is not None:
            self.set_check_plots(checks)
        else:
            self.checkList = None
    
    @classmethod
    def from_db(cls, imageLog, imageKeys, seCatKey, defaultsPath=None,
            configs=None, checks=None, workDir='scamp', useFileRefs=False):
        """Construct a SCAMP run from an image log database."""
        records = imageLog.get_images(imageKeys, [seCatKey])
        seCatPaths = [records[imageKey][seCatKey] for imageKey in imageKeys]
        
        return cls(seCatPaths, imageLog=imageLog, imageKeys=imageKeys,
            defaultsPath=defaultsPath, configs=configs, checks=checks,
            workDir=workDir, useFileRefs=False)
    
    def set_check_plots(self, checks):
        """Initializes the scamp checkplot types, and associated file names.
        
        By default, the file names are lower-cased versions of the check type,
        and the files are placed in the workDir.
        """
        self.checkList = checks
        self.checkPaths = []
        for checkType in self.checkList:
            self.checkPaths.append(os.path.join(self.workDir,
                checkType.lower()))
    
    def refcatalog_paths(self):
        """Returns the paths to existing reference catalogs."""
        catSource = self.configs["ASTREF_CATALOG"]
        catWildcard = "*".join((catSource, ".cat"))
        paths = glob.glob(os.path.join(self.workDir, catWildcard))
        return paths
    
    def make_command(self):
        """Writes the command to run scamp, and returns as a string."""
        
        # Make/add the default parametes file to the configs
        self.add_default_param_to_configs("scamp -d")
        
        # Write the input FITS file list to disk
        listPath = self.write_input_file_list(self.catalogPaths)
        
        # Form command with inputfile
        command = "scamp @%s" % listPath
        
        # If we're using pre-downloaded reference files, set this now
        if self.useFileRefs is True:
            # FIXME this is weird, refcatalog_paths() relied on ASTREF_CATALOG
            # being the name of the catalog source, and that useFileRefs is
            # the only thing that tells us to use a FILE source. Do something
            # more robust? e.g. the next two lines cannot be interchanged
            # in their ordering.
            refPaths = self.refcatalog_paths()
            self.add_to_configs('ASTREF_CATALOG', 'FILE')
            self.add_to_configs('ASTREFCAT_NAME', ",".join(refPaths))
        
        # Append checkplots
        if self.checkList is not None:
            checkPathArgs = ",".join(self.checkPaths)
            checkTypeArgs = ",".join(self.checkList)
            self.add_to_configs('CHECKPLOT_TYPE', checkTypeArgs)
            self.add_to_configs('CHECKPLOT_NAME', checkPathArgs)
        else:
            self.add_to_configs("CHECKPLOT_TYPE", "NONE")
        
        # Append all other configurations
        configCmd = self.make_config_command()
        if configCmd is not None:
            command = " ".join((command, configCmd))
        
        return command
    
    def save_scamp_headers(self, scampKey="scamp"):
        """Insert the SExtractor objects from the `self.headDB` dictionary to
        the given `imageLog` under the key `scampKey`. Must have instantiated
        Scamp with Scamp.from_db() so that the image keys are known.
        """
        print "//////"
        print "////// save_scamp_headers /////////"
        print "//////"
        for imageKey, headPath in self.headDB.iteritems():
            self.imageLog.set(imageKey, scampKey, headPath)


class SourceExtractor(Astromatic):
    """Represents a run of Terapix's Source Extractor."""
    
    def __init__(self, fitsPath, catalogName, weightPath=None,
            weightType=None, psfPath=None, configs=None, workDir="sex",
            defaultsPath="se/config.sex"):
        self.inputPath = fitsPath  # path to FITS image to be source-extracted
        self.catalogName = catalogName
        self.psfPath = psfPath
        self.weightPath = weightPath
        self.weightType = weightType
        
        super(SourceExtractor, self).__init__(configs=configs, workDir=workDir,
            defaultsPath=defaultsPath)
        
        self.checkList = []
        
        # Set-up the check images
        # if checks is not None:
        #     self.set_check_images(checks, self.workDir)
        # else:
        #     self.checkList = None
        
        self.catalogPath = os.path.join(workDir, catalogName + ".fits")
    
    def set_check_images(self, checkList, checkDir):
        """Sets which check images should be made, and where they should be
        saved. By default, all checkimages retain the base file name, plus an
        appropriate extension.
        """
        self.checkList = checkList
        self.checkPaths = []
        checkExt = {"SEGMENTATION": "seg", "OBJECTS": "obj",
                "MINIBACK_RMS": "minibrms",
                "BACKGROUND": "bkg", "MINIBACKGROUND": "minibkg",
                "BACKGROUND_RMS": "backrms", "-OBJECTS": "objsub"}
        baseName = os.path.splitext(os.path.basename(self.inputPath))[0]
        for checkType in self.checkList:
            self.checkPaths.append(os.path.join(self.workDir,
                "%s_%s.fits" % (baseName, checkExt[checkType])))
        # TODO add assert in case the user suppies a bad check image type
    
    def check_path(self, checkType):
        """Returns the path to a check image of type checkType. Returns None
        if no such check image exists.
        """
        print checkType
        print self.checkList
        print self.checkList.index(checkType)
        i = self.checkList.index(checkType)
        if i >= 0:
            path = self.checkPaths[i]
            return path
        else:
            return None
    
    def set_weight_file(self, weightPath, weightType):
        """Sets a weight image as a FITS at *weightPath* of SE weight image
        type *weightType*.
        """
        # TODO add ability to work with separate weight and detection maps
        self.weightPath = weightPath
        self.weightType = weightType
    
    def make_command(self):
        """Makes the source extractor command (for CL execution).
        Returns a string.
        """
        command = "sex %s" % self.inputPath
        
        # Add default parameters to the configs file. Writes a new defaults
        # if one is not found
        self.add_default_param_to_configs("sex -d")
        
        # Add CATALOG_NAME is to configs dictionary
        self.add_to_configs('CATALOG_NAME', self.catalogPath)
        
        # Add all check image info to the configs dictionary
        if len(self.checkList) > 0:
            checkPathArgs = ",".join(self.checkPaths)
            checkTypeArgs = ",".join(self.checkList)
            self.add_to_configs('CHECKIMAGE_TYPE', checkTypeArgs)
            self.add_to_configs('CHECKIMAGE_NAME', checkPathArgs)
        else:
            self.add_to_configs('CHECKIMAGE_TYPE', "NONE")
        
        # Add weight images
        if (self.weightPath is not None) and (self.weightType is not None):
            self.add_to_configs("WEIGHT_IMAGE", self.weightPath)
            self.add_to_configs("WEIGHT_TYPE", self.weightType)
        
        # Use a PSF
        if self.psfPath is not None:
            self.add_to_configs("PSF_NAME", self.psfPath)
        
        # Append all other configurations
        configCmd = self.make_config_command()
        if configCmd is not None:
            command = " ".join((command, configCmd))
        
        return command
    
    def catalog_path(self):
        """Returns the path to the SE catalog."""
        return self.catalogPath


class BatchSourceExtractor(object):
    """Run Source Extractor in parallel on multiple processors. ImageLog is
    used to store results from each run."""
    def __init__(self, imageLog, imageKeys, imagePathKey, catalogKey,
            weightKey=None, weightType=None, psfKey=None, configs={},
            workDir="se", defaultsPath="se/config.sex", catPostfix="se_cat"):
        super(BatchSourceExtractor, self).__init__()
        self.imageLog = imageLog
        self.imageKeys = imageKeys
        self.pathKey = imagePathKey
        self.catalogKey = catalogKey
        self.weightKey = weightKey
        self.psfKey = psfKey
        self.configs = configs
        self.weightType = weightType
        self.workDir = workDir
        self.defaultsPath = defaultsPath
        self.catPostfix = catPostfix
        self.checkDir = None
        self.checkKeyDict = {}
    
    def set_check_images(self, checkKeyDict, checkDir):
        """
        :param checkKeyDict: dictionary specifying SE check image types, and
            the image log keys to store paths to those images under.
            Keys are check type (`SEGMENTATION`, etc), values are image log
            keys.
        """
        self.checkDir = checkDir
        if os.path.exists(self.checkDir) is False:
            os.makedirs(self.checkDir)
        self.checkKeyDict = checkKeyDict
    
    def run(self, nthreads=multiprocessing.cpu_count(), debug=False):
        """docstring for run"""
        args = []
        for imageKey in self.imageKeys:
            imagePath = self.imageLog[imageKey][self.pathKey]
            if self.weightKey is not None:
                weightPath = self.imageLog[imageKey][self.weightKey]
            else:
                weightPath = None
            if self.psfKey is not None:
                psfPath = self.imageLog[imageKey][self.psfKey]
            else:
                psfPath = None
            if self.checkKeyDict is None:
                checkList = None
            else:
                checkList = self.checkKeyDict.keys()
            args.append((imageKey, imagePath, weightPath, self.weightType,
                psfPath, self.configs, checkList, self.catPostfix,
                self.workDir, self.defaultsPath))
        
        if debug is False:
            pool = multiprocessing.Pool(processes=nthreads)
            results = pool.map(_workSE, args)
        else:
            results = map(_workSE, args)
        
        # Insert results into the image log
        for (imageKey, se) in results:
            print imageKey, se
            self.imageLog.set(imageKey, self.catalogKey, se.catalog_path())
            for checkType, checkKey in self.checkKeyDict.iteritems():
                self.imageLog.set(imageKey, checkKey, se.check_path(checkType))


def _workSE(args):
    """Worker function for batch source extraction."""
    imageKey, imagePath, weightPath, weightType, psfPath, configs, \
        checkImages, catPostfix, workDir, defaultsPath = args
    
    catalogName = "_".join((str(imageKey), catPostfix))
    se = SourceExtractor(imagePath, catalogName, weightPath=weightPath,
        weightType=weightType, psfPath=psfPath, configs=configs,
        workDir=workDir,
        defaultsPath=defaultsPath)
    if checkImages is not None:
        se.set_check_images(checkImages, workDir)
    se.run()
    return imageKey, se


class BatchPSFex(object):
    def __init__(self, imageLog, groupedImageKeys, catalogPathKey, psfKey,
            configs=None, checkImages=None, checkPlots=None,
            defaultsPath=None, xmlKey=None, workDir="psfex", nThreads=None):
        """Runs multiple PSFex instances over independent groups of image keys.
        
        :param imageLog: a ImageLog-compatible database instance.
        :param groupedImageKeys: a dictionary of groupName: sequence of image
            keys
        :param catalogPathKey: record field where the SE (Source Extractor)
            catalogs are found for each image
        :param configs: (optional) dictionary of PSFex settings. Keys and
            values are standard PSFex command line arguments.
        :param defaultsPath: path to where a PSFex configuration file can be
            found
        :param xmlKey: key to install XML. If `None`, then PSFex will
            not produce XML output.
        :param workDir: directory where PSFex outputs are saved.
        :param nThreads: number of processes to run if `debug` is `False`.
        """
        self.imageLog = imageLog
        self.groupedImageKeys = groupedImageKeys
        self.catalogPathKey = catalogPathKey
        self.psfKey = psfKey
        self.xmlKey = xmlKey
        self.configs = configs
        self.checkImages = checkImages
        self.checkPlots = checkPlots
        self.defaultsPath = defaultsPath
        self.workDir = workDir
        if nThreads is not None:
            self.nThreads = nThreads
        else:
            self.nThreads = multiprocessing.cpu_count()
    
    def run(self, debug=False):
        """Executes the multiprocessing PSFex run."""
        dbArgs = {"url": self.imageLog.url,
                  "port": self.imageLog.port,
                  "dbname": self.imageLog.dbname,
                  "cname": self.imageLog.cname}
        
        args = []
        for groupName, imageKeys in self.groupedImageKeys.iteritems():
            args.append((groupName, imageKeys, self.catalogPathKey,
                self.psfKey, self.configs, self.checkImages, self.checkPlots,
                self.defaultsPath, self.xmlKey, self.workDir, dbArgs))
        
        if debug:
            map(_run_batch_psfex, args)
        else:
            pool = multiprocessing.Pool(processes=self.nThreads)
            pool.map(_run_batch_psfex, args)


def _run_batch_psfex(args):
    """Worker function for executing PSFex from BatchPSFex.
    
    The path to the PSF is stored under `psfKey`."""
    groupName, imageKeys, catalogPathKey, psfKey, configs, checkImages, \
        checkPlots, defaultsPath, xmlKey, workDir, dbArgs = args
    dbArgs = dict(dbArgs)  # in case we run in debug mode
    dbname = dbArgs.pop('dbname')
    cname = dbArgs.pop('cname')
    imageLog = ImageLog(dbname, cname, **dbArgs)
    
    print "Running imageKeys:", imageKeys
    psfex = PSFex.from_db(imageLog, imageKeys, catalogPathKey, configs=configs,
        xmlKey=xmlKey, defaultsPath=None, workDir=workDir)
    if checkImages is not None:
        psfex.set_check_images(checkImages, "psfex",
                os.path.join(workDir, "checks"))
    if checkPlots is not None:
        psfex.set_check_plots(checkPlots, "psfex",
                os.path.join(workDir, "plots"), plotType="PSC")
    psfex.run()
    psfex.save_psf_paths(psfKey)
    if xmlKey is not None:
        psfex.save_xml_paths()


class PSFex(Astromatic):
    """Wrapper on the PSFex PSF-modelling software."""
    def __init__(self, catalogPaths, imageLog=None, imageKeys=None,
            defaultsPath=None, configs=None, xmlKey=None,
            workDir='psfex', groupName=None):
        super(PSFex, self).__init__()
        if type(catalogPaths) is str:
            self.catalogPaths = [catalogPaths]
        else:
            self.catalogPaths = catalogPaths
        self.imageLog = imageLog
        self.imageKeys = imageKeys
        self.configs = configs
        self.xmlKey = xmlKey
        self.workDir = workDir
        # allows the input list to be named different in batch mode
        self.groupName = groupName
        if self.groupName is None:
            self.groupName = self.imageKeys[0]
        
        self.checkDir = None
        self.checkList = None
        self.checkPaths = None
        
        self.plotDir = None
        self.plotList = None
        self.plotPaths = None
    
    @classmethod
    def from_db(cls, imageLog, imageKeys, catalogPathKey,
            configs=None, xmlKey=None, defaultsPath=None, workDir='psfex'):
        """Creates a PSFex run from an image log database.
        
        :param imageLog: a ImageLog-compatible database instance.
        :param imageKeys: image keys in the imageLog where SE catalogs will be
            gathered.
        :param catalogPathKey: record field where the SE (Source Extractor)
            catalogs are found for each image
        :param xmlKey: key to install XML. If `None`, then PSFex will
            not produce XML output.
        :param configs: (optional) dictionary of PSFex settings. Keys and
            values are standard PSFex command line arguments.
        :param defaultsPath: path to where a PSFex configuration file can be
            found
        :param workDir: directory where PSFex outputs are saved.
        """
        docs = imageLog.find({"_id": {"$in": imageKeys}},
                fields=[catalogPathKey])
        imageKeys = []
        catalogPaths = []
        for d in docs:
            imageKeys.append(d['_id'])
            catalogPaths.append(d[catalogPathKey])
        return cls(catalogPaths, imageLog=imageLog, imageKeys=imageKeys,
            defaultsPath=defaultsPath, configs=configs, xmlKey=xmlKey,
            workDir=workDir)
    
    def set_check_images(self, checkList, prefix, checkDir):
        """Sets which check images should be made, and where they should be
        saved. By default, all checkimages retain the base file name, plus
        and appropriate extension.
        
        :param checkList: sequence of check image types. May include:
        * CHI
        * PROTOTYPES
        * SAMPLES
        * RESIDUALS
        * SNAPSHOTS
        * MOFFAT
        * -MOFFAT
        * -SYMMETRICAL
        * BASIS
        :param prefix: filename prefix for all check images.
        :param checkDir: directory where the check images should be saved.
        """
        self.checkDir = checkDir
        if os.path.exists(checkDir) is False: os.makedirs(checkDir)
        self.checkList = checkList
        
        print prefix
        print self.checkDir
        checkPath = {"CHI": "chi", "PROTOTYPES": "proto", "SAMPLES": "samp",
            "RESIDUALS": "resi", "SNAPSHOTS": "snap", "MOFFAT": "moffat",
            "-MOFFAT": "-moffat", "-SYMMETRICAL": "-symm", "BASIS": "basis"}
        if self.checkList is not None:
            self.checkPaths = [os.path.join(self.checkDir,
                "%s_%s.fits" % (prefix, checkPath[c])) for c in self.checkList]
        else:
            self.checkPaths = None
        print self.checkPaths
    
    def set_check_plots(self, plotList, prefix, plotDir, plotType="PDF"):
        """Sets which check figures (plplot) should be made, and where they
        should be saved.
        
        :param checkList: sequence of check figure types. May include:
        * FWHM - map of PSF FWHM over field
        * ELLIPTICITY - map of PSF ellipticity over field
        * COUNTS - map of spatial density of point sources
        * COUNT_FRACTION - map of fraction of points accepted for
            PSF evaluation
        * CHI2 - map of average chi^2/d.o.f over field
        * MOFFAT_RESIDUALS - map of moffat residuals
        * ASYMMETRY - map of asymmetry indices
        """
        self.plotDir = plotDir
        if os.path.exists(plotDir) is False: os.makedirs(plotDir)
        self.plotList = plotList
        plotPath = {"FWHM": "fwhm", "ELLIPTICITY": "ellip", "COUNTS": 'counts',
            "COUNT_FRACTION": 'cfrac', "CHI2": "chi",
            "MOFFAT_RESIDUALS": 'resid', "ASYMMETRY": 'asym'}
        if self.plotList is not None:
            self.plotPaths = [os.path.join(self.plotDir,
                "%s_%s.fits" % (prefix, plotPath[c])) for c in self.plotList]
        else:
            self.plotPaths = None
        self.plotType = plotType
    
    def make_command(self):
        """Makes the source extractor command (for CL execution).
        Returns a string.
        """
        # Automatically make XML if we can install it
        self.xmlPath = os.path.join(self.workDir, self.groupName + ".xml")
        if self.xmlKey is None:
            self.configs['WRITE_XML'] = "N"
        else:
            self.configs['WRITE_XML'] = "Y"
            self.configs['XML_NAME'] = self.xmlPath

        # If more than 10 catalogs are being processed, a file list is written
        if len(self.catalogPaths) > 10:
            if self.groupName is not None:
                inputName = "%s_input_catalogs.txt" % self.groupName
            else:
                inputName = "input_catalogs.txt"
            inputListPath = os.path.join(self.workDir, inputName)
            if os.path.exists(inputListPath): os.remove(inputListPath)
            f = open(inputListPath, 'w')
            f.write("\n".join(self.catalogPaths) + "\n")
            f.close()
            command = "psfex @%s" % inputListPath
        elif len(self.catalogPaths) == 1:
            command = "psfex %s" % self.catalogPaths[0]
        else:
            command = "psfex %s" % ",".join(self.catalogPaths)
        
        # Add default parameters to the configs file. Writes a new defaults
        # if one is not found
        self.add_default_param_to_configs("psfex -d")
        
        # Add all check image info to the configs dictionary
        if self.checkList is not None:
            checkImageArgs = ",".join(self.checkList)
            checkPathArgs = ",".join(self.checkPaths)
            self.add_to_configs('CHECKIMAGE_TYPE', checkImageArgs)
            self.add_to_configs('CHECKIMAGE_NAME', checkPathArgs)
        
        # Add all check plot info to the configs dictionary
        if self.plotList is not None:
            plotArgs = ",".join(self.plotList)
            plotPathArgs = ",".join(self.plotPaths)
            self.add_to_configs("CHECKPLOT_TYPE", plotArgs)
            self.add_to_configs("CHECKPLOT_NAME", plotPathArgs)
            self.add_to_configs("CHECKPLOT_DEV", self.plotType)
        
        # Append all other configurations
        configCmd = self.make_config_command()
        if configCmd is not None:
            command = " ".join((command, configCmd))
        
        return command
    
    def check_path(self, checkType):
        """Returns the path to the specified type of check iamage."""
        i = self.checkList.index(checkType)
        if i >= 0:
            path = self.checkPaths[i]
            return path
        else:
            return None
    
    def psf_paths(self):
        """Get the paths to each image, in the same order as the input
        catalog paths.
        """
        psfPaths = [os.path.join(self.workDir,
            os.path.splitext(os.path.basename(path))[0] + ".psf")
            for path in self.catalogPaths]
        return psfPaths
    
    def save_psf_paths(self, psfKey):
        """Files the psf file paths into the image log under `psfKey` for
        all images.
        """
        for imageKey, catPath in zip(self.imageKeys, self.catalogPaths):
            psfPath = os.path.join(self.workDir,
                    os.path.splitext(os.path.basename(catPath))[0] + ".psf")
            self.imageLog.set(imageKey, psfKey, psfPath)

    def save_xml_paths(self):
        """Files the xml filepaths into image log under `xmlKey` for all
        images."""
        if self.xmlKey is None: return
        for imageKey in self.imageKeys:
            self.imageLog.set(imageKey, self.xmlKey, self.xmlPath)
