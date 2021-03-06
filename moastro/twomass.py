#!/usr/bin/env python
# encoding: utf-8
"""
Access the 2MASS survey as a local MongoDB.
"""

import os
import glob
import gzip

import pymongo
from pymongo import ASCENDING, GEO2D
import numpy as np
from astropy.wcs import WCS
import astropy.units as u
from astropy.coordinates import SkyCoord

from .dbtools import make_connection


PSC_FORMAT = (('ra',float),('dec',float),('err_maj',float),('err_min',float),
    ('err_ang',int),('designation',unicode),('j_m',float),('j_cmsig',float),
    ('j_msigcom',float),('j_snr',float),('h_m',float),('h_cmsig',float),
    ('h_msigcom',float),('h_snr',float),('k_m',float),('k_cmsig',float),
    ('k_msigcom',float),('k_snr',float),('ph_qual',unicode),('rd_flg',unicode),
    ('bl_flg',unicode),('cc_flg',unicode),('ndet',unicode),('prox',float),
    ('pxpa',int),('pxcntr',int),('gal_contam',int),('mp_flg',int),
    ('pts_key',int),('hemis',unicode),('date date',unicode),('scan',int),
    ('glon',float),('glat',float),('x_scan',float),('jdate',float),
    ('j_psfchi',float),('h_psfchi',float),('k_psfchi',float),
    ('j_m_stdap',float),('j_msig_stdap',float),('h_m_stdap',float),
    ('h_msig_stdap',float),('k_m_stdap',float),('k_msig_stdap',float),
    ('dist_edge_ns',int),('dist_edge_ew',int),('dist_edge_flg',unicode),
    ('dup_src',int),('use_src',int),('a',unicode),('dist_opt',float),
    ('phi_opt',int),('b_m_opt',float),('vr_m_opt',float),
    ('nopt_mchs',int),('ext_key',int),('scan_key',int),('coadd_key',int),
    ('coadd',int))


class PSC(object):
    """2MASS Point Source Catalog representation in MongoDB.
    
    Parameters
    ----------

    dbname : str
        Name of MongoDB database.
    cname : str
        Name of MongoDB collection.
    server : str
        Name of the MongoDB server, as specified in ``~/.moastro.json``. If
        ``None``, then the values of ``url`` and ``port`` will be adopted
        instead.
    url : str
        URL of MongoDB server.
    port : int
        Port of MongoDB server.
    """
    def __init__(self, dbname='twomass', cname='psc',
            server=None, url="localhost", port=27017):
        conn = make_connection(server=server, url=url, port=port)
        db = conn[dbname]
        self.c = db[cname]

        self.default_fields = ['coord', 'j_m', 'h_m', 'k_m']

    @classmethod
    def import_psc(cls, f, dbname="twomass", cname="psc", 
            server=None, url="localhost", port=27017,
            drop=False, center=None, radius=None,
            fields=('j_m', 'j_cmsig', 'j_msigcom', 'j_snr',
                    'h_m', 'h_cmsig', 'h_msigcom', 'h_snr',
                    'k_m', 'k_cmsig', 'k_msigcom', 'k_snr')):
        """Build a PSC database in MongoDB from the ascii data streams.
        
        Parameters
        ----------

        f : file-like object
            A file-like object conforming to the 2MASS PSC file spec.
        dbname : str
            Name of MongoDB database.
        cname : str
            Name of MongoDB collection.
        server : str
            Name of the MongoDB server, as specified in ``~/.moastro.json``. If
            ``None``, then the values of ``url`` and ``port`` will be adopted
            instead.
        url : str
            URL of MongoDB server.
        port : int
            Port of MongoDB server.
        drop : bool
            Set to `True` if any existing PSC collection should be dropped
            useful for re-doing an import.
        """
        conn = make_connection(server=server, url=url, port=port)
        db = conn[dbname]
        if drop:
            db.drop_collection(cname)
        collection = db[cname]
        
        colourIndices = (('j_m','h_m'),('j_m','k_m'),('h_m','k_m'))
        
        #f = open(dataPath, 'r')
        docs = []
        ra_coords = []
        dec_coords = []
        for line in f:
            line.strip() # strip newline
            items = line.split("|")
            doc = {}
            for (name, dtype), item in zip(PSC_FORMAT, items):
                if item == "\N": continue # ignore null values
                # Don't add spatial quantities directly; storing (RA,Dec) and
                # (long, lat) as tuples lets us make geospatial indices
                if name == "ra":
                    ra = dtype(item)
                elif name == "dec":
                    dec = dtype(item)
                elif name == "glon":
                    glon = dtype(item)
                elif name == "glat":
                    glat = dtype(item)
                elif name in fields:
                    doc[name] = dtype(item)
            # Insert geospatial fields
            ra_coords.append(ra)
            dec_coords.append(dec)
            doc['coord'] = (ra, dec)
            doc['galactic'] = (glon,glat)
            # Pre-compute colours
            for (c1, c2) in colourIndices:
                colourName = "%s-%s" % (c1, c2)
                if (c1 in doc) and (c2 in doc):
                    doc[colourName] = doc[c1] - doc[c2]
            docs.append(doc)
        ra_coords = np.array(ra_coords) * u.degree
        dec_coords = np.array(dec_coords) * u.degree
        star_coords = SkyCoord(ra_coords, dec_coords)
        if center is not None:
            selected = np.where(center.separation(star_coords) < radius)[0]
            for i in selected:
                collection.insert(docs[i])
        else:
            for doc in docs:
                collection.insert(doc)

    @classmethod
    def index_space_color(cls, dbname="twomass", cname="psc",
            server=None, url="localhost", port=27017):
        """Generates an geospatial+colour+magnitude index.
        
        The index is constructed so that RA,Dec is indexed first as this
        is most useful in using 2MASS for targeted applications.
        """
        conn = make_connection(server=server, url=url, port=port)
        db = conn[dbname]
        collection = db[cname]

        collection.ensure_index([("coord",GEO2D),("j_m-k_m",ASCENDING),
            ("k_m",ASCENDING),("j_m",ASCENDING),("h_m",ASCENDING),
            ("h_m",ASCENDING),("h_m-k_m",ASCENDING),("j_m-h_m",ASCENDING)],
            min=-90., max=360., name="radec_color", background=True)

    def find(self, spec, fields=[], center=None, radius=None, box=None,
            polygon=None, header=None, wcs=None):
        """General purpose query method for 2MASS PSC.

        .. todo:: Use exceptions to make spatial query resolution chain
           more robust.
        
        Parameters
        ----------
        spec : dict
            A `pymongo` query specification. Note that spatial query
            parameters will override those passed in spec.
        fields : list
            List of PSC fields to return (in addition to
            `self.default_fields`.)
        center: (2,) tuple or list
            For spherical spatial queries, defines the query center as
            an (RA, Dec.) tuple in degrees.
        radius: float (degrees)
            Radius of the spherical query, in degrees. Use with `center`.
        polygon: list of [RA,Dec] vertices in degrees. The polygon is
            automatically closed.
        box: list or tuple `[[RA_min,Dec_min],[RA_max,Dec_max]]`
            Queries for stars inside a rectangular range of RA and Dec. Assumes
            decimal degrees for RA and Dec.
        header: `pyfits.header` instance
            Queries stars within the footprint defined by the `pyfits` image
            header.
        wcs: pywcs.WCS instance
            Queries stars within the footprint defined by the WCS.
        
        Returns
        -------
        recs : `pymongo.Cursor` instance
            The cursor can be iterated to access each star. Stars are
            represented as dictionaries whose keys are the requested
            data `fields`.

        Notes
        -----
        Only one type of spatial query is performed, even if several are
        defined by the keyword arguments passed by the user. The spatial
        query is resolved in the following order:

        1. wcs
        2. header
        3. polygon
        4. box
        5. center and radius

        Examples
        --------
        To query for all stars with :math:`J-K_s > 0.5` mag within 2 degrees
        of M31, and returning only the RA,Dec position, :math:`J` magnitude
        and :math:`K_s` magnitude:

        >>> psc = PSC()
        >>> recs = psc.find({"j_m-k_m": {"$gt": 0.5}},
                fields=["coord","j_m","k_m"],
                center=(13.,41.), radius=2.)
        """
        if wcs is not None:
            spatialSpec = self._make_spatial_wcs(wcs)
        elif header is not None:
            spatialSpec = self._make_spatial_header(header)
        elif polygon is not None:
            spatialSpec = {"coord": {"$within": {"$polygon": polygon}}}
        elif box is not None:
            spatialSpec = {"coord": {"$within": {"$box": box}}}
        elif center is not None and radius is not None:
            spatialSpec = {"coord": {"$within": {"$center": [center,radius]}}}
        else:
            spatialSpec = {}
        spec.update(spatialSpec)
        print "2MASS query:", spec
        getFields = self.default_fields + fields
        return self.c.find(spec, getFields)

    def _make_spatial_wcs(self, wcs):
        """Make a spatial query spec from a PyWCS WCS instance."""
        poly = wcs.calc_footprint() # (4,2) numpy array
        allRA = [float(c[0]) for c in poly]
        allDec = [float(c[1]) for c in poly]
        verts = zip(allRA, allDec)
        return {"coord": {"$within": {"$polygon": verts}}}

    def _make_spatial_header(self, header):
        """Make a spatial query spec from a PyFITS header instance."""
        wcs = WCS(header)
        return self._make_spatial_wcs(wcs)


def test_import_psc(testPath, host="localhost", port=27017, dbname="twomass",
        cname="psc", drop=True):
    """Import the test_psc practice file."""
    f = open(testPath, 'r')
    PSC.import_psc(f, drop=drop)
    f.close()


def import_compressed_psc(dataDir, host="localhost", port=27017,
        dbname="twomass", cname="psc", center=None, radius=None):
    """Import decompressed PSC text catalogs from dataDir.
    
    The psc collection is dropped before this operation.
    """
    filePaths = glob.glob(os.path.join(dataDir, "psc_*.gz"))
    drop = True
    for filePath in filePaths:
        print "Loading %s" % filePath
        print "Search from", center
        print "Radius", radius
        f = gzip.open(filePath, 'rb') # decompress on the file
        # Decompress the file
        PSC.import_psc(f, drop=drop, center=center, radius=radius)
        f.close()
        drop = False # only drop on first file import!
    PSC.index_space_color()


def reset_psc(dbname="twomass", cname="psc"):
    """Drops the 2MASS PSC collection!"""
    db = pymongo.Connection()[dbname]
    db.drop_collection(cname)


if __name__ == '__main__':
    pass
