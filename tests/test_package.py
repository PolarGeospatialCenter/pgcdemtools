import argparse
import glob
import os
import platform
import shutil
import subprocess
import unittest

import pytest
from osgeo import gdal, gdalconst

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

__test_dir__ = os.path.dirname(__file__)
testdata_dir = os.path.join(__test_dir__, 'testdata')
__app_dir__ = os.path.dirname(__test_dir__)

@pytest.mark.skipif(platform.system() == "Windows", reason="Not applicable on Windows")
class TestPackagerStrips(unittest.TestCase):

    def setUp(self):
        self.script_name = 'package_setsm.py'
        self.source_data_dn = 'setsm_strip_packager'
        self.source_data = os.path.join(testdata_dir, self.source_data_dn)
        self.output_dir = os.path.join(__test_dir__, 'tmp_output')
        os.makedirs(self.output_dir, exist_ok=True)

    def tearDown(self):
        ## Clean up output
        shutil.rmtree(self.output_dir, ignore_errors=True)

    def testOutput(self):

        o_list = [
            'SETSM_s2s041_WV01_20221223_10200100D0A07B00_10200100D1CD1900_2m_seg1',
            'SETSM_s2s041_WV02_20221220_10300100DF1F1500_10300100DF27F000_2m_seg1',
            'SETSM_s2s041_WV02_20221220_10300100DF1F1500_10300100DF27F000_2m_seg2',
            'SETSM_s2s041_WV03_20220927_104001007C72BD00_104001007D43CB00_2m_lsf_seg1'
        ]

        test_param_list = (
            # input, [expected outputs], args, message
            (self.source_data, o_list, '--project arcticdem --build-rasterproxies', None),
            (self.source_data, o_list, '--skip-archive', None),
        )
        j=0
        for i, o_list, opts, msg in test_param_list:
            j+=1
            o_dir = os.path.join(self.output_dir, f'{self.source_data_dn}_{j}')
            # clean up test area if needed
            try:
                shutil.rmtree(o_dir)
            except:
                pass
            # link source to test area
            shutil.copytree(self.source_data, o_dir, copy_function=os.link)
            # run test
            cmd = f'python {__app_dir__}/{self.script_name} {o_dir} {o_dir} {opts}'

            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so, se) = p.communicate()
            # print(se)
            # print(so)

            ## Test assertions
            for o in o_list:
                ob = os.path.join(o_dir, o)
                ## Test that tar and dem.mrf exist
                if 'skip-archive' not in opts:
                    self.assertTrue(os.path.isfile(f'{ob}.tar.gz'))
                else:
                    self.assertFalse(os.path.isfile(f'{ob}.tar.gz'))

                if 'build-rasterproxies' in opts:
                    self.assertTrue(os.path.isfile(f'{ob}_dem.mrf'))
                else:
                    self.assertFalse(os.path.isfile(f'{ob}_dem.mrf'))

                ## Test that tifs are COGs
                for tif in glob.glob(f'{ob}*tif'):
                    if 'dem_10m.tif' not in tif:
                        ds = gdal.Open(tif, gdalconst.GA_ReadOnly)
                        self.assertIn('LAYOUT=COG', ds.GetMetadata_List('IMAGE_STRUCTURE'))

            ## Test if stdout has proper text
            if msg:
                try:
                    self.assertIn(msg, so.decode())
                except AssertionError as e:
                    self.assertIn(msg, se.decode())

@pytest.mark.skipif(platform.system() == "Windows", reason="Not applicable on Windows")
class TestPackagerTiles(unittest.TestCase):

    def setUp(self):
        self.script_name = 'package_setsm_tiles.py'
        self.source_data_dn = 'setsm_tile_packager'
        self.source_data = os.path.join(testdata_dir, self.source_data_dn)
        self.output_dir = os.path.join(__test_dir__, 'tmp_output')
        os.makedirs(self.output_dir, exist_ok=True)

    def tearDown(self):
        ## Clean up output
        shutil.rmtree(self.output_dir, ignore_errors=True)

    # @unittest.skip("test")
    def testOutput(self):

        o_list = [
            '10_27_2_2_2m_v4.1',
            '59_57_1_1_2m',
        ]

        test_param_list = (
            # input, [expected outputs], args, message
            (self.source_data, o_list,
              '--project arcticdem --epsg 3413 --build-rasterproxies', None),
        )

        for i, o_list, opts, msg in test_param_list:
            o_dir = os.path.join(self.output_dir, self.source_data_dn)
            # clean up test area if needed
            try:
                shutil.rmtree(o_dir)
            except:
                pass
            # link source to test area
            shutil.copytree(self.source_data, o_dir, copy_function=os.link)
            # run test
            cmd = f'python {__app_dir__}/{self.script_name} {o_dir} {o_dir} {opts}'

            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so, se) = p.communicate()
            # print(se)
            # print(so)

            ## Test assertions
            for o in o_list:
                ob = os.path.join(o_dir, o)
                ## Test that tar exists
                self.assertTrue(os.path.isfile(f'{ob}.tar.gz'))
                self.assertTrue(os.path.isfile(f'{ob}_dem.mrf'))

                ## Test that tifs are COGs
                for tif in glob.glob(f'{ob}*tif'):
                    if 'countmt.tif' not in tif:
                        ds = gdal.Open(tif, gdalconst.GA_ReadOnly)
                        self.assertIn('LAYOUT=COG', ds.GetMetadata_List('IMAGE_STRUCTURE'))

            ## Test if stdout has proper text
            if msg:
                try:
                    self.assertIn(msg, so.decode())
                except AssertionError as e:
                    self.assertIn(msg, se.decode())


if __name__ == '__main__':

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Functional tests for package_setsm.py and package_setsm_tiles.py"
    )

    #### Parse Arguments
    args = parser.parse_args()

    test_cases = [
        TestPackagerStrips,
        TestPackagerTiles,
    ]

    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)

    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
