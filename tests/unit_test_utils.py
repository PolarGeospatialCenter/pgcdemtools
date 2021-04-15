import argparse
import logging
import os
import sys
import unittest

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
root_dir = os.path.dirname(script_dir)
sys.path.append(root_dir)

from lib import utils

logger = logging.getLogger("logger")
# lso = logging.StreamHandler()
# lso.setLevel(logging.ERROR)
# formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
# lso.setFormatter(formatter)
# logger.addHandler(lso)
        

class TestCopyDems(unittest.TestCase):
    
    def setUp(self):
        self.asp_strip_files = [
            #### strip
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DEM.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-PC.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-PC-center.txt",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-PC.las",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-PC.laz",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-GoodPixelMap.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DEM.prj",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DRG.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-IntersectionErr.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00_fltr-DEM.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00_fltr-DEM.prj",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.geojson",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-stereo.default",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shp",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.dbf",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shx",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.prj",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-log-stereo_corr",
        ]
        
        self.asp_scene_files = [
            #### scene
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DEM.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-PC.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-PC-center.txt",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-PC.las",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-PC.laz",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-GoodPixelMap.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DEM.prj",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DRG.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-IntersectionErr.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001_fltr-DEM.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001_fltr-DEM.prj",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001.geojson",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-stereo.default",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-log-stereo_corr",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shp",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.dbf",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shx",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.prj",
        ]
        
        self.setsm_strip_files = [
            #### strip
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m_dem.geom",
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m_dem.tif",
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m_matchtag.tif",
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m_matchtag_browse.tif",
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m_ortho.tif",
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m_ortho_8bit.tif",
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m_meta.txt",
        ]
        
        self.args = DemArgs()
        self.asp_pairname = 'WV01_20120422_102001001AE38C00_102001001B0AAD00'
        self.setsm_pairname = 'WV01_20080830_1020010003561200_1020010004712300'
    
    def test_copy_asp_strip(self):
        
        files_to_move = [
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DEM.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-PC.las",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-PC.laz",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-GoodPixelMap.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DEM.prj",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DRG.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-IntersectionErr.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.geojson",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-stereo.default",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shp",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.dbf",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shx",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.prj",
        ]
        
        overlap_prefix = self.asp_pairname
        for f in self.asp_strip_files:
            move_file = utils.check_file_inclusion(f, self.asp_pairname, overlap_prefix, self.args)
            if f in files_to_move:
                self.assertTrue(move_file)
            else:
                self.assertFalse(move_file)
                
    def test_copy_asp_strip_include_all(self):
        
        files_to_move = [
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DEM.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-PC.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-PC-center.txt",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-PC.las",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-PC.laz",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-GoodPixelMap.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DEM.prj",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DRG.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-IntersectionErr.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00_fltr-DEM.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00_fltr-DEM.prj",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.geojson",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-stereo.default",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shp",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.dbf",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shx",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.prj",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-log-stereo_corr",
        ]
        
        self.args.include_fltr = True
        self.args.include_logs = True
        self.args.include_pc = True
        overlap_prefix = self.asp_pairname
        for f in self.asp_strip_files:
            move_file = utils.check_file_inclusion(f, self.asp_pairname, overlap_prefix, self.args)
            if f in files_to_move:
                self.assertTrue(move_file)
            else:
                self.assertFalse(move_file)
                
    def test_copy_asp_strip_exclude_drg_and_err(self):
        
        files_to_move = [
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DEM.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-PC.las",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-PC.laz",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-GoodPixelMap.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DEM.prj",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.geojson",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-stereo.default",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shp",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.dbf",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shx",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.prj",
        ]
        
        self.args.exclude_drg = True
        self.args.exclude_err = True
        overlap_prefix = self.asp_pairname
        for f in self.asp_strip_files:
            move_file = utils.check_file_inclusion(f, self.asp_pairname, overlap_prefix, self.args)
            if f in files_to_move:
                self.assertTrue(move_file)
            else:
                self.assertFalse(move_file)
                
    def test_copy_asp_strip_dems_only(self):
        
        files_to_move = [
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DEM.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DEM.prj",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.geojson",
        ]
        
        self.args.dems_only = True
        overlap_prefix = self.asp_pairname
        for f in self.asp_strip_files:
            move_file = utils.check_file_inclusion(f, self.asp_pairname, overlap_prefix, self.args)
            if f in files_to_move:
                self.assertTrue(move_file)
            else:
                self.assertFalse(move_file)
        
    def test_copy_asp_strip_dems_only_and_include_fltr(self):
        
        files_to_move = [
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DEM.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00-DEM.prj",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.geojson",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00_fltr-DEM.tif",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00_fltr-DEM.prj",
        ]
        
        self.args.include_fltr = True
        self.args.dems_only = True
        overlap_prefix = self.asp_pairname
        for f in self.asp_strip_files:
            move_file = utils.check_file_inclusion(f, self.asp_pairname, overlap_prefix, self.args)
            if f in files_to_move:
                self.assertTrue(move_file)
            else:
                self.assertFalse(move_file)

    def test_copy_asp_scene(self):
        
        files_to_move = [
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DEM.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-PC.las",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-PC.laz",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-GoodPixelMap.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DEM.prj",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DRG.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-IntersectionErr.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001.geojson",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-stereo.default",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shp",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.dbf",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shx",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.prj",
        ]
        
        overlap_prefix = "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001"
        for f in self.asp_scene_files:
            move_file = utils.check_file_inclusion(f, self.asp_pairname, overlap_prefix, self.args)
            if f in files_to_move:
                self.assertTrue(move_file)
            else:
                self.assertFalse(move_file)
                
    def test_copy_asp_scene_include_all(self):
        
        files_to_move = [
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DEM.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-PC.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-PC-center.txt",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-PC.las",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-PC.laz",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-GoodPixelMap.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DEM.prj",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DRG.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-IntersectionErr.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001_fltr-DEM.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001_fltr-DEM.prj",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001.geojson",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-stereo.default",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-log-stereo_corr",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shp",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.dbf",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shx",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.prj",
        ]
        
        self.args.include_fltr = True
        self.args.include_logs = True
        self.args.include_pc = True
        overlap_prefix = "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001"
        for f in self.asp_scene_files:
            move_file = utils.check_file_inclusion(f, self.asp_pairname, overlap_prefix, self.args)
            if f in files_to_move:
                self.assertTrue(move_file)
            else:
                self.assertFalse(move_file)
                
    def test_copy_asp_scene_exclude_drg_and_err(self):
        
        files_to_move = [
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DEM.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-PC.las",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-PC.laz",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-GoodPixelMap.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DEM.prj",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001.geojson",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-stereo.default",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shp",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.dbf",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.shx",
            "WV01_20120422_102001001AE38C00_102001001B0AAD00.prj",
        ]
        
        self.args.exclude_drg = True
        self.args.exclude_err = True
        overlap_prefix = "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001"
        for f in self.asp_scene_files:
            move_file = utils.check_file_inclusion(f, self.asp_pairname, overlap_prefix, self.args)
            if f in files_to_move:
                self.assertTrue(move_file)
            else:
                self.assertFalse(move_file)
                
    def test_copy_asp_scene_dems_only(self):
        
        files_to_move = [
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DEM.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DEM.prj",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001.geojson",
        ]
        
        self.args.dems_only = True
        overlap_prefix = "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001"
        for f in self.asp_scene_files:
            move_file = utils.check_file_inclusion(f, self.asp_pairname, overlap_prefix, self.args)
            if f in files_to_move:
                self.assertTrue(move_file)
            else:
                self.assertFalse(move_file)
        
    def test_copy_asp_scene_dems_only_and_include_fltr(self):
        
        files_to_move = [
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DEM.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001-DEM.prj",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001_fltr-DEM.tif",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001_fltr-DEM.prj",
            "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001.geojson",
        ]
        
        self.args.include_fltr = True
        self.args.dems_only = True
        overlap_prefix = "WV01_20120422153705_102001001AE38C00_12APR22153705-P1BS-052895840020_01_P001_WV01_20120422153618_102001001B0AAD00_12APR22153618-P1BS-052895834010_01_P001"
        for f in self.asp_scene_files:
            move_file = utils.check_file_inclusion(f, self.asp_pairname, overlap_prefix, self.args)
            if f in files_to_move:
                self.assertTrue(move_file)
            else:
                self.assertFalse(move_file)

    def test_copy_setsm(self):
        files_to_move = [
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_1_8m_dem.tif",
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_1_8m_matchtag.tif",
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_1_8m_ortho.tif",
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_1_8m_meta.txt",
        ]
        
        overlap_prefix = "SETSM_WV01_20080830_1020010003561200_1020010004712300_1_8m"
        for f in self.setsm_strip_files:
            move_file = utils.check_file_inclusion(f, self.setsm_pairname, overlap_prefix, self.args)
            if f in files_to_move:
                self.assertTrue(move_file)
            else:
                self.assertFalse(move_file)
                
    def test_copy_setsm_exclude_drg(self):
        files_to_move = [
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m_dem.tif",
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m_matchtag.tif",
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m_meta.txt",
        ]
        
        self.args.exclude_drg = True
        overlap_prefix = "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m"
        for f in self.setsm_strip_files:
            move_file = utils.check_file_inclusion(f, self.setsm_pairname, overlap_prefix, self.args)
            if f in files_to_move:
                self.assertTrue(move_file)
            else:
                self.assertFalse(move_file)
                
    def test_copy_setsm_dems_only(self):
        files_to_move = [
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m_dem.tif",
            "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m_meta.txt",
        ]
        
        self.args.dems_only = True
        overlap_prefix = "SETSM_WV01_20080830_1020010003561200_1020010004712300_seg1_8m"
        for f in self.setsm_strip_files:
            move_file = utils.check_file_inclusion(f, self.setsm_pairname, overlap_prefix, self.args)
            if f in files_to_move:
                self.assertTrue(move_file)
            else:
                self.assertFalse(move_file)

    
class DemArgs(object):
    def __init__(self):
        self.exclude_drg = False
        self.dems_only = False
        self.exclude_err = False
        self.include_pc = False
        self.include_fltr = False
        self.include_logs = False
        self.tar_only = False

        

if __name__ == '__main__':
    
    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Test utils package"
        )

    #### Parse Arguments
    args = parser.parse_args()
        
    test_cases = [
        TestCopyDems,
    ]
    
    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)
    
    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
