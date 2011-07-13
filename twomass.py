#!/usr/bin/env python
# encoding: utf-8
"""
Access the 2MASS survey as a local MongoDB.

History
-------
2011-07-11 - Created by Jonathan Sick

"""

__all__ = ['']

import os
import glob

import pymongo

import numpy as np
import pylab as pl

import auth

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
    """2MASS Point Source Catalog representation in MongoDB."""
    def __init__(self, host="localhost", port=27017, dbname="twomass",
            cname="psc"):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.cname = cname

        conn = pymongo.Connection(host=host, port=port)
        cred = auth.Credentials()
        db = cred.connect_db(dbname, c=conn)
        self.collection = db[cname]

    @classmethod
    def import_psc(cls, dataPath, host="localhost", port=27017, dbname="twomass",
            cname="psc", drop=False):
        """Build a PSC database in MongoDB from the ascii data sources"""
        c = pymongo.Connection(host=host, port=port)
        cred = auth.Credentials()
        db = cred.connect_db("twomass", c=c)
        collection = db[cname]
        
        f = open(dataPath, 'r')
        for line in f:
            line.strip() # strip newline
            items = line.split("|")
            doc = {}
            for (name, dtype), item in zip(PSC_FORMAT, items):
                if item == "\N": continue
                if name == "ra": ra = dtype(item)
                elif name == "dec": dec = dtype(item)
                elif name == "glon": glon = dtype(item)
                elif name == "glat": glat = dtype(item)
                else: doc[name] = dtype(item)
            doc['coord'] = (ra, dec)
            doc['galactic'] = (glon,glat)
            collection.insert(doc)
        f.close()
        collection.ensure_index([("coord",pymongo.GEO2D),
            ("min",-90.), ("max",360.)])
        PSC.generate_colors(collection)

    @classmethod
    def generate_colors(self, collection):
        """Generate J-H, J-K and H-K color indices."""
        indices = (('j_m','h_m'),('j_m','k_m'),('h_m','k_m'))
        for (c1,c2) in indices:
            colourName = "%s-%s" % (c1, c2)
            spec = {c1: {"$exists": True}, c2: {"$exists": True},
                    colourName: {"$exists": True}}
            recs = collection.find(spec, fields=[c1,c2])
            print "Making %s colour for %i stars" % (colourName.upper(),
                    recs.count())
            for rec in recs:
                colour = rec[c1] - rec[c2]
                collection.update(rec['_id'], {"$set": {colourName: colour}})


    def find(self, spec):
        """docstring for find"""
        pass




def test_import_psc(testPath, host="localhost", port=27017, dbname="twomass",
        cname="psc", drop=True):
    """Import the test_psc practice file."""
    PSC.import_psc(testPath, drop=drop)

def import_decompressed_psc(dataDir):
    """Import decompressed PSC text catalogs from dataDir.
    
    The psc collection is dropped before this operation.
    """
    filePaths = glob.glob(os.path.join(dataDir, "psc_*"))
    drop = True
    for filePath in filePaths:
        basename = os.path.split(filePath)[-1]
        if len(basename) != 7: continue # only look at decompressed files
        print "Loading %s" % filePath
        PSC.import_psc(filePath, drop=drop)
        drop=False

if __name__ == '__main__':
    #test_import_psc("/Volumes/Zaphod/m31/data/2mass_psc/practice/test_psc")
    #import_decompressed_psc("/Volumes/Zaphod/m31/data/2mass_psc")
    psc = PSC()
    PSC.generate_colors(psc.collection)


