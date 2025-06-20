import argparse
import os
import platform
import shutil
import subprocess
import unittest

from osgeo import ogr

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

__test_dir__ = os.path.dirname(__file__)
testdata_dir = os.path.join(__test_dir__, 'testdata')
__app_dir__ = os.path.dirname(__test_dir__)

res_str = {
    2.0: '_2m_v',
    0.5: '_50cm_v',
}

class TestIndexerScenes(unittest.TestCase):

    def setUp(self):
        self.scene_dir = os.path.join(testdata_dir, 'setsm_scene')
        self.scene_json_dir = os.path.join(testdata_dir, 'setsm_scene_json')
        self.scene50cm_dir = os.path.join(testdata_dir, 'setsm_scene_50cm')
        self.scenedsp_dir = os.path.join(testdata_dir, 'setsm_scene_2mdsp')
        self.output_dir = os.path.join(__test_dir__, 'tmp_output')
        self.test_str = os.path.join(self.output_dir, 'test.shp')
        self.pg_test_str = 'PG:sandwich:test_pgcdemtools'
        os.makedirs(self.output_dir, exist_ok=True)

        self.scene_count = 52
        self.scene_json_count = 43
        self.scene50cm_count = 14
        self.scenedsp_count = 102

    def tearDown(self):
        ## Clean up output
        shutil.rmtree(self.output_dir, ignore_errors=True)

    # @unittest.skip("test")
    def testOutputShp(self):

        ## Build shp
        test_param_list = (
            # input, output, args, result feature count, message
            (self.scene_dir, self.test_str, '--skip-region-lookup', self.scene_count, 'Done'),  # test creation
            (self.scene_dir, self.test_str, '--skip-region-lookup --append', self.scene_count * 2, 'Done'),  # test append
            (self.scene_dir, self.test_str, '--skip-region-lookup', self.scene_count * 2,
             'Dst shapefile exists.  Use the --overwrite or --append options.'),  # test error message on existing
            (self.scene_dir, self.test_str, '--skip-region-lookup --overwrite', self.scene_count, 'Removing old index'), # test overwrite
            (self.scene_dir, self.test_str, '--skip-region-lookup --overwrite --check', self.scene_count, 'Done'), # test check
            (self.scene_dir, self.test_str, '--dsp-record-mode both --skip-region-lookup --overwrite',
             self.scene_count, 'Done'),  # test dsp-record-mode both has no effect when record is not dsp
            (self.scene_json_dir, self.test_str, '--skip-region-lookup --overwrite --read-json', self.scene_json_count,
             'Done'), # test old jsons
        )

        for i, o, options, result_cnt, msg in test_param_list:
            cmd = 'python {}/index_setsm.py --np {} {} {}'.format(
                __app_dir__,
                i,
                o,
                options
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so, se) = p.communicate()
            # print(se)
            # print(so)

            ## Test if ds exists and has correct number of records
            self.assertTrue(os.path.isfile(o))
            ds = ogr.Open(o, 0)
            layer = ds.GetLayer()
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt, result_cnt)
            for feat in layer:
                srcfn = os.path.basename(feat.GetField('LOCATION'))
                is_xtrack = 0 if srcfn.startswith(('WV', 'GE', 'QB')) else 1
                self.assertEqual(feat.GetField('IS_XTRACK'), is_xtrack)
                self.assertIsNotNone(feat.GetField('PROD_VER'))
                record_res = feat.GetField('DEM_RES')
                has_lsf = bool(feat.GetField("HAS_LSF"))
                has_nonlsf = bool(feat.GetField("HAS_NONLSF"))
                if record_res == 0.5:
                    self.assertTrue(has_nonlsf)
                    self.assertFalse(has_lsf)
            ds, layer = None, None

            ## Test if stdout has proper error
            try:
                self.assertIn(msg, so.decode())
            except AssertionError as e:
                self.assertIn(msg, se.decode())

    # @unittest.skip("test")
    def testCustomPaths(self):

        self.test_str = os.path.join(self.output_dir, 'test.gdb', 'test_lyr')

        pairname_region_lookup = {
            'WV02_20190419_103001008C4B0400_103001008EC59A00': ('arcticdem_05_greenland_northeast', 'arcgeu'),
            'WV02_20190705_103001009505B700_10300100934D1000': ('arcticdem_10_canada_north_mainland', 'arcnam'),
            'W1W1_20190426_102001008466F300_1020010089C2DB00': ('arcticdem_02_greenland_southeast', 'arcgeu'),
            'WV01_20120317_10200100192B8400_102001001AC4FE00': ('arcticdem_07_canada_ellesmere', 'arcnam'),
            'WV02_20140330_103001002F22FF00_103001002E1D1C00': ('arcticdem_20_russia_kamchatka', 'arcasa'),
            'WV02_20220813_10300100D7D7F300_10300100D86CC000': ('arcticdem_03_greenland_southwest', 'arcgeu'),
        }

        PROJECTS = {
            'arcticdem': 'ArcticDEM',
            'rema': 'REMA',
            'earthdem': 'EarthDEM',
        }

        ## Build shp
        test_param_list = (
            # input, output, args, result feature count, message
            (self.scene_dir, self.test_str, '--read-pickle {}/tests/testdata/pair_region_lookup.p --custom-paths BP --check'.format(__app_dir__),
             self.scene_count, 'Done'), # test BP paths
            (self.scene_dir, self.test_str, '--read-pickle {}/tests/testdata/pair_region_lookup.p --overwrite --custom-paths PGC'.format(__app_dir__),
             self.scene_count, 'Done'),  # test PGC paths
            (self.scene_dir, self.test_str, '--skip-region-lookup --overwrite --custom-paths CSS',
             self.scene_count, 'Done'),  # test CSS paths
            (self.scenedsp_dir, self.test_str, '--read-pickle {}/tests/testdata/pair_region_lookup.p --overwrite --custom-paths BP --check'.format(__app_dir__),
             self.scenedsp_count, 'Done'),  # test 2m_dsp record
            (self.scenedsp_dir, self.test_str, '--read-pickle {}/tests/testdata/pair_region_lookup.p --overwrite --custom-paths PGC'.format(__app_dir__),
             self.scenedsp_count, 'Done'),  # test 2m_dsp record
        )

        for i, o, options, result_cnt, msg in test_param_list:
            cmd = 'python {}/index_setsm.py --np {} {} {}'.format(
                __app_dir__,
                i,
                o,
                options
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so, se) = p.communicate()
            print(se)
            print(so)

            ## Test if ds exists and has correct number of records
            gdb, lyr = os.path.split(o)
            self.assertTrue(os.path.isdir(gdb))
            ds = ogr.Open(gdb, 0)
            layer = ds.GetLayerByName(lyr)
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt, result_cnt)
            for feat in layer:
                location = feat.GetField('LOCATION')
                pairname = feat.GetField('PAIRNAME')
                res = feat.GetField('DEM_RES')
                is_dsp = feat.GetField('IS_DSP')
                res_str2 = '2m' if res == 2.0 else '50cm'
                res_dir = res_str2 + '_dsp' if is_dsp else res_str2
                if '--custom-paths BP' in options:
                    p = 'https://blackpearl-data2.pgc.umn.edu/dem-scenes-{}-{}/{}/W'.format(
                        res_str2, pairname_region_lookup[pairname][1], res_dir)
                    self.assertTrue(location.startswith(p))
                elif '--custom-paths PGC' in options:
                    r = pairname_region_lookup[pairname][0]
                    p = '/mnt/pgc/data/elev/dem/setsm/{}/region/{}/scenes/{}/W'.format(
                        PROJECTS[r.split('_')[0]], r, res_dir)
                    self.assertTrue(location.startswith(p))
                elif '--custom-paths CSS' in options:
                    p = '/css/nga-dems/setsm/scene/{}/W'.format(res_dir)
                    self.assertTrue(location.startswith(p))

            ds, layer = None, None

            # Test if stdout has proper error
            self.assertIn(msg, so.decode())

    # @unittest.skip("test")
    def testOutputGdb(self):

        self.test_str = os.path.join(self.output_dir, 'test.gdb', 'test_lyr')

        ## Build shp
        test_param_list = (
            # input, output, args, result feature count, message
            (self.scene_dir, self.test_str, '', self.scene_count, 'Done'),  # test creation
            (self.scene_dir, self.test_str, '--append', self.scene_count * 2, 'Done'),  # test append
            (self.scene_dir, self.test_str, '', self.scene_count * 2,
            'Dst GDB layer exists.  Use the --overwrite or --append options.'),  # test error message on existing
            (self.scene_dir, self.test_str, '--overwrite --check', self.scene_count, 'Removing old index'), # test overwrite
        )

        for i, o, options, result_cnt, msg in test_param_list:
            cmd = 'python {}/index_setsm.py --np {} {} --skip-region-lookup {}'.format(
                __app_dir__,
                i,
                o,
                options
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so, se) = p.communicate()
            # print(se)
            # print(so)

            ## Test if ds exists and has correct number of records
            gdb, lyr = os.path.split(o)
            self.assertTrue(os.path.isdir(gdb))
            ds = ogr.Open(gdb, 0)
            layer = ds.GetLayerByName(lyr)
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt, result_cnt)
            ds, layer = None, None

            ## Test if stdout has proper error
            try:
                self.assertIn(msg, so.decode())
            except AssertionError as e:
                self.assertIn(msg, se.decode())

    # @unittest.skip("test")
    def testScene50cm(self):

        ## Build shp
        test_param_list = (
            # input, output, args, result feature count, message
            (self.scene50cm_dir, self.test_str, '', self.scene50cm_count, 'Done'),  # test creation
        )

        for i, o, options, result_cnt, msg in test_param_list:
            cmd = 'python {}/index_setsm.py --np {} {} --skip-region-lookup {}'.format(
                __app_dir__,
                i,
                o,
                options
            )

            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so, se) = p.communicate()
            # print(se)
            # print(so)

            ## Test if ds exists and has correct number of records
            self.assertTrue(os.path.isfile(o))
            ds = ogr.Open(o, 0)
            layer = ds.GetLayer()
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt, result_cnt)
            for feat in layer:
                record_res = feat.GetField('DEM_RES')
                has_lsf = feat.GetField("HAS_LSF")
                has_nonlsf = feat.GetField("HAS_NONLSF")
                if record_res == 0.5:
                    self.assertTrue(has_nonlsf)
                    self.assertFalse(has_lsf)
                elif record_res == 2.0:
                    self.assertTrue(has_lsf)
                    if feat.GetField("CENT_LAT") < -60:
                        self.assertFalse(has_lsf)
                    else:
                        self.assertTrue(has_nonlsf)
            ds, layer = None, None

            ##Test if stdout has proper error
            self.assertIn(msg, so.decode())

    # @unittest.skip("test")
    def testSceneDsp(self):

        ## Build shp
        test_param_list = (
            # input, output, args, result feature count, message
            (self.scenedsp_dir, self.test_str, '--dsp-record-mode dsp --skip-region-lookup', self.scenedsp_count, 'Done', 2),  # test as 2m_dsp record
            (self.scenedsp_dir, self.test_str, '--overwrite --dsp-record-mode orig --skip-region-lookup', self.scenedsp_count, 'Done',
             0.5),  # test as 50cm record
            (self.scenedsp_dir, self.test_str, '--overwrite --dsp-record-mode both --skip-region-lookup', self.scenedsp_count*2,
            'Done', None),  # test as 50cm and 2m records
            (self.scenedsp_dir, self.test_str, '--overwrite --dsp-record-mode both --status-dsp-record-mode-orig aws --skip-region-lookup',
            self.scenedsp_count * 2, 'Done', None),  # test as 50cm and 2m records with custom status
            (self.scenedsp_dir, self.test_str, '--overwrite --custom-paths BP --dsp-record-mode both --status-dsp-record-mode-orig aws --read-pickle {}/tests/testdata/pair_region_lookup.p'.format(__app_dir__),
             self.scenedsp_count * 2, 'Done', None),  # test as 50cm and 2m records with Bp paths and custom status
        )

        for i, o, options, result_cnt, msg, res in test_param_list:
            cmd = 'python {}/index_setsm.py --np {} {} {}'.format(
                __app_dir__,
                i,
                o,
                options
            )

            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so, se) = p.communicate()
            # print(se)
            # print(so)

            ## Test if ds exists and has correct number of records
            self.assertTrue(os.path.isfile(o))
            ds = ogr.Open(o, 0)
            layer = ds.GetLayer()
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt, result_cnt)
            for feat in layer:
                scenedemid = feat.GetField('SCENEDEMID')
                stripdemid = feat.GetField('STRIPDEMID')
                location = feat.GetField('LOCATION')
                record_res = feat.GetField('DEM_RES')
                scenedemid_lastpart = scenedemid.split('_')[-1]
                location_lastpart = location.split('_')[-2]
                if '-' in location_lastpart:
                    self.assertEqual(scenedemid_lastpart.split('-')[1], location_lastpart.split('-')[1])
                if res:
                    self.assertEqual(record_res, res)
                    self.assertTrue(scenedemid_lastpart.startswith('2' if res == 2.0 else '0'))
                    self.assertTrue(res_str[res] in stripdemid)
                    self.assertEqual(feat.GetField('IS_DSP'), 1 if res == 2.0 else 0)
                self.assertTrue(scenedemid_lastpart.startswith('2' if record_res == 2.0 else '0'))
                self.assertEqual(feat.GetField('IS_DSP'), 1 if record_res == 2.0 else 0)
                if '--status-dsp-record-mode-orig aws' in options:
                    if '--custom-paths BP' in options:
                        self.assertEqual(feat.GetField('STATUS'), 'aws' if record_res == 0.5 else 'tape')
                    else:
                        self.assertEqual(feat.GetField('STATUS'), 'aws' if record_res == 0.5 else 'online')

                # TODO revert to all records using assertIsNotNone after all incorrect 50cminfo.txt files are ingested
                if record_res == 0.5:
                    self.assertIsNone(feat.GetField('FILESZ_DEM'))
                else:
                    self.assertIsNotNone(feat.GetField('FILESZ_DEM'))

            ds, layer = None, None

            # Test if stdout has proper error
            self.assertIn(msg, so.decode())

    # @unittest.skip("test")
    def testSceneJson(self):

        ## Test json creation
        cmd = 'python {}/index_setsm.py --np {} {} --write-json'.format(
            __app_dir__,
            self.scene_dir,
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so, se) = p.communicate()
        print(se)
        print(so)

        jsons = [
            os.path.join(self.output_dir, 'WV02_20190419_103001008C4B0400_103001008EC59A00_2m_v040002.json'),
            os.path.join(self.output_dir, 'WV02_20190705_103001009505B700_10300100934D1000_2m_v040002.json'),
            os.path.join(self.output_dir, 'W1W1_20190426_102001008466F300_1020010089C2DB00_2m_v030403.json'),
            os.path.join(self.output_dir, 'WV02_20220813_10300100D7D7F300_10300100D86CC000_2m_v040311.json'),
        ]

        counter = 0
        for json in jsons:
            self.assertTrue(os.path.isfile(json))
            fh = open(json)
            for line in fh:
                cnt = line.count('sceneid')
                counter += cnt
        self.assertEqual(counter, self.scene_count)

        ## Test json exists error
        msg = 'Json file already exists'
        cmd = 'python {}/index_setsm.py --np {} {} --write-json'.format(
            __app_dir__,
            self.scene_dir,
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so, se) = p.communicate()
        print(se)
        print(so)

        self.assertIn(msg, so.decode())

        ## Test json overwrite
        stat = os.stat(os.path.join(self.output_dir, 'WV02_20190419_103001008C4B0400_103001008EC59A00_2m_v040002.json'))
        mod_date1 = stat.st_mtime

        if not platform.system() == 'Windows': # will fail on windows because file is not released.  Just skip it.
            cmd = 'python {}/index_setsm.py --np {} {} --write-json --overwrite'.format(
                __app_dir__,
                self.scene_dir,
                self.output_dir,
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so, se) = p.communicate()
            print(se)
            print(so)

            stat = os.stat(os.path.join(self.output_dir, 'WV02_20190419_103001008C4B0400_103001008EC59A00_2m_v040002.json'))
            mod_date2 = stat.st_mtime
            self.assertGreater(mod_date2, mod_date1)

        ## Test json read
        test_shp = os.path.join(self.output_dir, 'test.shp')
        cmd = 'python {}/index_setsm.py --np {} {} --skip-region-lookup --read-json --check'.format(
            __app_dir__,
            self.output_dir,
            test_shp,
        )

        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so, se) = p.communicate()
        print(se)
        print(so)

        self.assertTrue(os.path.isfile(test_shp))
        ds = ogr.Open(test_shp, 0)
        layer = ds.GetLayer()
        self.assertIsNotNone(layer)
        cnt = layer.GetFeatureCount()
        self.assertEqual(cnt, self.scene_count)
        for feat in layer:
            record_res = feat.GetField('DEM_RES')
            has_lsf = feat.GetField("HAS_LSF")
            has_nonlsf = feat.GetField("HAS_NONLSF")
            if record_res == 0.5:
                self.assertTrue(has_nonlsf)
                self.assertFalse(has_lsf)
            elif record_res == 2.0:
                self.assertTrue(has_lsf)
                if feat.GetField("CENT_LAT") < -60:
                    self.assertFalse(has_lsf)
                else:
                    self.assertTrue(has_nonlsf)
        ds, layer = None, None

    # @unittest.skip("test")
    def testSceneDspJson(self):

        test_param_list = (
            ('', self.scenedsp_count, 2.0),
            ('--dsp-record-mode orig --overwrite', self.scenedsp_count, 0.5),
            ('--dsp-record-mode both --overwrite', self.scenedsp_count * 2, None),
        )

        ## Test json creation
        cmd = 'python {}/index_setsm.py --np {} {} --write-json'.format(
            __app_dir__,
            self.scenedsp_dir,
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so, se) = p.communicate()
        # print(se)
        # print(so)

        json1 = os.path.join(self.output_dir, 'WV01_20120317_10200100192B8400_102001001AC4FE00_2m_v040201.json')
        self.assertTrue(os.path.isfile(json1))

        ## Test json read
        test_shp = os.path.join(self.output_dir, 'test.shp')
        for options, result_cnt, res in test_param_list:
            cmd = 'python {}/index_setsm.py --np {} {} {} --skip-region-lookup --read-json'.format(
                __app_dir__,
                self.output_dir,
                test_shp,
                options,
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so, se) = p.communicate()
            # print(se)
            # print(so)

            self.assertTrue(os.path.isfile(test_shp))
            ds = ogr.Open(test_shp, 0)
            layer = ds.GetLayer()
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt, result_cnt)
            feat = layer.GetFeature(1)
            scenedemid = feat.GetField('SCENEDEMID')
            stripdemid = feat.GetField('STRIPDEMID')
            scenedemid_lastpart = scenedemid.split('_')[-1]
            record_res = feat.GetField('DEM_RES')
            if res:
                self.assertEqual(feat.GetField('DEM_RES'), res)
                self.assertTrue(scenedemid_lastpart.startswith('2' if res == 2.0 else '0'))
                self.assertTrue(res_str[res] in stripdemid)
                self.assertEqual(feat.GetField('IS_DSP'), 1 if res == 2.0 else 0)
            self.assertEqual(feat.GetField('IS_DSP'), 1 if record_res == 2.0 else 0)
            self.assertTrue(scenedemid_lastpart.startswith('2' if record_res == 2.0 else '0'))

            # TODO revert to all records using assertIsNotNone after all incorrect 50cminfo.txt files are ingested
            if res == 0.5:
                self.assertIsNone(feat.GetField('FILESZ_DEM'))
            else:
                self.assertIsNotNone(feat.GetField('FILESZ_DEM'))

            ds, layer = None, None


class TestIndexerStrips(unittest.TestCase):

    def setUp(self):
        self.strip_dir = os.path.join(testdata_dir, 'setsm_strip')
        self.strip_json_dir = os.path.join(testdata_dir, 'setsm_strip_json')
        self.strip_mixedver_dir = os.path.join(testdata_dir, 'setsm_strip_mixedver')
        self.strip_mdf_dir = os.path.join(testdata_dir, 'setsm_strip_mdf')
        self.strip_txt_mdf_dir = os.path.join(testdata_dir, 'setsm_strip_txt_mdf')
        self.stripmasked_dir = os.path.join(testdata_dir, 'setsm_strip_masked')
        self.striprenamed_dir = os.path.join(testdata_dir, 'setsm_strip_renamed')
        self.output_dir = os.path.join(__test_dir__, 'tmp_output')
        self.test_str = os.path.join(self.output_dir, 'test.shp')
        self.pg_test_str = 'PG:sandwich:test_pgcdemtools'
        os.makedirs(self.output_dir, exist_ok=True)

        self.strip_count = 6
        self.stripmasked_count = 3
        self.strip_mixedver_count = 4
        self.strip_json_count = 6
        self.strip_renamed_count = 1

    def tearDown(self):
        ## Clean up output
        shutil.rmtree(self.output_dir, ignore_errors=True)

    # @unittest.skip("test")
    def testStrip(self):

        test_param_list = (
            # input, output, args, result feature count, message
            (self.strip_dir, self.test_str, '', self.strip_count, 'Done'),  # test creation
            (self.strip_json_dir, self.test_str, '--overwrite --read-json', self.strip_json_count, 'Done'),
            # test old json rebuild
            (self.strip_json_dir, self.test_str,
             '--overwrite --read-json --overwrite --project arcticdem --use-release-fields --lowercase-fieldnames',
             self.strip_json_count, 'Done'),
            # test old json rebuild with release fields
            (self.strip_mixedver_dir, self.test_str, '--overwrite', self.strip_mixedver_count, 'Done'),  # test mixed version
            (self.strip_mdf_dir, self.test_str, '--overwrite', self.strip_count,
             'WARNING- Strip DEM avg acquisition times not found'), # test rebuild from mdf
            (self.stripmasked_dir, self.test_str, '--overwrite --check', self.stripmasked_count, 'Done'), # test index of masked strips
            (self.stripmasked_dir, self.test_str, '--overwrite --search-masked', self.stripmasked_count * 5, 'Done'),  # test index of masked strips
            (self.striprenamed_dir, self.test_str, '--overwrite --project arcticdem --use-release-fields --lowercase-fieldnames',
             self.strip_renamed_count, 'Done')
        )

        strip_masks = {
            ## name: (edgemask, watermask, cloudmask)
            '_dem.tif': (1, 0, 0),
            '_dem_water-masked.tif': (1, 1, 0),
            '_dem_cloud-masked.tif': (1, 0, 1),
            '_dem_cloud-water-masked.tif': (1, 1, 1),
            '_dem_masked.tif': (1, 1, 1),
        }

        for i, o, options, result_cnt, msg in test_param_list:
            cmd = 'python {}/index_setsm.py --np --mode strip {} {} --skip-region-lookup {}'.format(
                __app_dir__,
                i,
                o,
                options
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so, se) = p.communicate()
            # print(cmd)
            # print(se)
            # print(so)

            ## Test if ds exists and has correct number of records
            self.assertTrue(os.path.isfile(o))
            ds = ogr.Open(o, 0)
            layer = ds.GetLayer()
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt, result_cnt)
            location_field = 'FILEURL' if '--use-release-fields' in options else 'LOCATION'
            for feat in layer:
                srcfp = feat.GetField(location_field)
                srcdir, srcfn = os.path.split(srcfp)
                srcfn_minus_prefix = '_'.join(srcfn.split('_')[2:]) if srcfn.startswith('SETSM_s2s') else srcfn
                dem_suffix = srcfn[srcfn.find('_dem'):]
                if not '--use-release-fields' in options:
                    stripdemid = feat.GetField('STRIPDEMID')
                    folder_stripdemid = os.path.basename(srcdir).replace('_lsf', '')
                    if len(folder_stripdemid.split('_')) > 5:
                        self.assertEqual(folder_stripdemid, stripdemid)
                    masks = strip_masks[dem_suffix]
                    self.assertEqual(feat.GetField('EDGEMASK'), masks[0])
                    self.assertEqual(feat.GetField('WATERMASK'), masks[1])
                    self.assertEqual(feat.GetField('CLOUDMASK'), masks[2])
                is_xtrack = False if srcfn_minus_prefix.startswith(('WV', 'GE', 'QB')) else True
                self.assertEqual(feat.GetField('IS_XTRACK'), is_xtrack)
            ds, layer = None, None

            ## Test if stdout has proper error
            try:
                self.assertIn(msg, so.decode())
            except AssertionError as e:
                self.assertIn(msg, se.decode())

    # @unittest.skip("test")
    def testStripFromTxtAndMdf(self):

        ## Test fields and values are identical (except location) when index is build from txt or
        ##   mdf, both with and without if --release-fields option
        opt_sets = [
            '',
            '--use-release-fields --lowercase-fieldnames --project rema',
        ]
        j=0
        for opts in opt_sets:
            j+=1
            test_str1 = os.path.join(self.output_dir, str(j), 'test.shp')
            test_str2 = os.path.join(self.output_dir, str(j), 'test2.shp')
            test_param_list = (
                # input, output
                (os.path.join(self.strip_txt_mdf_dir, 'txt'), test_str1),
                (os.path.join(self.strip_txt_mdf_dir, 'mdf'), test_str2),
            )
            for i, o in test_param_list:
                cmd = 'python {}/index_setsm.py --np --mode strip {} {} --skip-region-lookup {}'.format(
                    __app_dir__,
                    i,
                    o,
                    opts
                )
                os.makedirs(os.path.dirname(o), exist_ok=True)
                p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (so, se) = p.communicate()
                print(cmd)
                print(se)
                print(so)

            # Open each fc and compare field names and values
            self.assertTrue(os.path.isfile(test_str1))
            ds1 = ogr.Open(test_str1, 0)
            lyr1 = ds1.GetLayer()
            self.assertTrue(os.path.isfile(test_str2))
            ds2 = ogr.Open(test_str2, 0)
            lyr2 = ds2.GetLayer()
            for lyr in (lyr1, lyr2):
                self.assertIsNotNone(lyr)
                self.assertEqual(lyr.GetFeatureCount(),1)

            ldefn = lyr1.GetLayerDefn()
            flds = [ldefn.GetFieldDefn(n).name for n in range(ldefn.GetFieldCount())]
            vals = dict()
            i=0
            for feat in lyr1:
                i+=1
                vals[i] = [feat.GetField(j) for j in flds if j.lower() != 'location']

            ldefn2 = lyr1.GetLayerDefn()
            flds2 = [ldefn2.GetFieldDefn(n).name for n in range(ldefn2.GetFieldCount())]
            self.assertEqual(flds, flds2)
            i=0
            for feat in lyr2:
                i+=1
                vals2 = [feat.GetField(j) for j in flds if j.lower() != 'location']
                self.assertEqual(vals[1],vals2)

    # @unittest.skip("test")
    def testStripJson(self):
        ## Test json creation
        cmd = 'python {}/index_setsm.py --np {} {} --mode strip --write-json'.format(
            __app_dir__,
            self.strip_dir,
            self.output_dir,
        )

        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so, se) = p.communicate()
        # print(se)
        # print(so)

        json_list = (
            'WV01_20140402_102001002C6AFA00_102001002D8B3100_2m_lsf_v030202.json',
        )

        for json_fn in json_list:
            json = os.path.join(self.output_dir, json_fn)
            self.assertTrue(os.path.isfile(json))

        ## Test json read
        cmd = 'python {}/index_setsm.py --np {} {} --mode strip --skip-region-lookup --read-json'.format(
            __app_dir__,
            self.output_dir,
            self.test_str,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so, se) = p.communicate()
        # print(se)
        # print(so)

        self.assertTrue(os.path.isfile(self.test_str))
        ds = ogr.Open(self.test_str, 0)
        layer = ds.GetLayer()
        self.assertIsNotNone(layer)
        cnt = layer.GetFeatureCount()
        self.assertEqual(cnt, self.strip_count)
        ds, layer = None, None

    # @unittest.skip("test")
    def testStripCustomPaths(self):

        pairname_region_lookup = {
            'W1W1_20190426_102001008466F300_1020010089C2DB00': ('arcticdem_02_greenland_southeast', 'arcgeu'),
            'WV01_20140402_102001002C6AFA00_102001002D8B3100': ('arcticdem_01_iceland', 'arcgeu'),
            'WV01_20150425_102001003F9C6100_102001003D411100': ('arcticdem_01_iceland', 'arcgeu'),
            'WV01_20201202_102001009E86AB00_10200100A0B14900': ('earthdem_03_conus', 'nplnam'),
        }

        PROJECTS = {
            'arcticdem': 'ArcticDEM',
            'rema': 'REMA',
            'earthdem': 'EarthDEM',
        }

        ## Build shp
        test_param_list = (
            # input, output, args, result feature count, message
            (self.strip_dir, self.test_str, '--read-pickle {}/tests/testdata/pair_region_lookup.p --custom-paths BP'.format(__app_dir__),
             self.strip_count, 'Done'),  # test BP paths
            (self.strip_dir, self.test_str,
             '--read-pickle {}/tests/testdata/pair_region_lookup.p --overwrite --custom-paths PGC'.format(__app_dir__),
             self.strip_count, 'Done'),  # test PGC paths
            (self.strip_dir, self.test_str,
             '--read-pickle {}/tests/testdata/pair_region_lookup.p --skip-region-lookup --overwrite --custom-paths CSS'.format(__app_dir__),
             self.strip_count,
             'Done'),  # test CSS paths
        )

        for i, o, options, result_cnt, msg in test_param_list:
            cmd = 'python {}/index_setsm.py --np --mode strip {} {} {}'.format(
                __app_dir__,
                i,
                o,
                options
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so, se) = p.communicate()
            # print(cmd)
            # print(se)
            # print(so)

            ## Test if ds exists and has correct number of records
            self.assertTrue(os.path.isfile(o))
            ds = ogr.Open(o, 0)
            layer = ds.GetLayer()
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt, result_cnt)
            for feat in layer:
                location = feat.GetField('LOCATION')
                pairname = feat.GetField('PAIRNAME')
                res = feat.GetField('DEM_RES')
                s2s_version = feat.GetField('S2S_VER')
                #is_dsp = feat.GetField('IS_DSP')
                res_dir = '2m' if res == 2.0 else '50cm'
                #res_dir = res_dir + '_dsp' if is_dsp else res_dir
                if '--custom-paths BP' in options:
                    # FIXME: Will we need separate buckets for different s2s version strips (i.e. v4 vs. v4.1)?
                    p = 'https://blackpearl-data2.pgc.umn.edu/dem-strips-{}/{}/W'.format(
                        pairname_region_lookup[pairname][1], res_dir)
                    self.assertTrue(location.startswith(p))
                elif '--custom-paths PGC' in options:
                    r = pairname_region_lookup[pairname][0]
                    p = '/mnt/pgc/data/elev/dem/setsm/{}/region/{}/strips_v{}/{}/W'.format(
                        PROJECTS[r.split('_')[0]], r, s2s_version, res_dir)
                    self.assertTrue(location.startswith(p))
                elif '--custom-paths CSS' in options:
                    p = '/css/nga-dems/setsm/strip/strips_v{}/{}/W'.format(s2s_version, res_dir)
                    self.assertTrue(location.startswith(p))

            ds, layer = None, None

            # Test if stdout has proper error
            self.assertIn(msg, so.decode())


class TestIndexerTiles(unittest.TestCase):

    def setUp(self):
        self.tile_dir = os.path.join(testdata_dir, 'setsm_tile')
        self.output_dir = os.path.join(__test_dir__, 'tmp_output')
        self.test_str = os.path.join(self.output_dir, 'test.shp')
        self.pg_test_str = 'PG:sandwich:test_pgcdemtools'
        os.makedirs(self.output_dir, exist_ok=True)

    def tearDown(self):
        ## Clean up output
        shutil.rmtree(self.output_dir, ignore_errors=True)

    # @unittest.skip("test")
    def testTile(self):

        test_param_list = (
            # input, output, args, result feature count, message
            (os.path.join(self.tile_dir, 'v3', '33_11'), self.test_str,
             '--project arcticdem', 3, 'Done'),  # test 100x100km tile at 3 resolutions
            (os.path.join(self.tile_dir, 'v3', '33_11_quartertiles'), self.test_str,
             '--overwrite --project arcticdem', 4, 'Done'),  # test quartertiles formatted for release
            (os.path.join(self.tile_dir, 'v4', '59_57'), self.test_str,
             '--overwrite --check --project arcticdem', 4, 'Done'),  # test v4 tiles, 2m
            (os.path.join(self.tile_dir, 'v4', 'utm34n_60_06'), self.test_str,
             '--overwrite --project earthdem', 4, 'Done'),  # test v4 utm tiles, 2m
        )

        for i, o, options, result_cnt, msg in test_param_list:
            cmd = 'python {}/index_setsm.py --np --mode tile  {} {} {}'.format(
                __app_dir__,
                i,
                o,
                options
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so, se) = p.communicate()
            # print(se)
            # print(so)

            ## Test if ds exists and has correct number of records
            self.assertTrue(os.path.isfile(o))
            ds = ogr.Open(o, 0)
            layer = ds.GetLayer()
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt, result_cnt)
            ds, layer = None, None

            ## Test if stdout has proper error
            self.assertIn(msg, so.decode())

    # @unittest.skip("test")
    def testTileJson(self):
        ## Test json creation
        cmd = 'python {}/index_setsm.py --np {} {} --mode tile --project arcticdem --write-json'.format(
            __app_dir__,
            os.path.join(self.tile_dir, 'v3', '33_11'),
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so, se) = p.communicate()
        # print(cmd)
        # print(se)
        # print(so)

        json_list = [
            'arcticdem_33_11_2m.json',
            'arcticdem_33_11_40m.json',
            'arcticdem_33_11_10m.json',
        ]

        for json_fn in json_list:
            json = os.path.join(self.output_dir, json_fn)
            self.assertTrue(os.path.isfile(json))

        ## Test json read
        cmd = 'python {}/index_setsm.py --np {} {} --mode tile --project arcticdem --read-json'.format(
            __app_dir__,
            self.output_dir,
            self.test_str,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so, se) = p.communicate()
        # print(cmd)
        # print(se)
        # print(so)

        self.assertTrue(os.path.isfile(self.test_str))
        ds = ogr.Open(self.test_str, 0)
        layer = ds.GetLayer()
        self.assertIsNotNone(layer)
        cnt = layer.GetFeatureCount()
        self.assertEqual(cnt, 3)
        ds, layer = None, None

    # @unittest.skip("test")
    def testTilev4Json(self):
        ## Test json creation
        cmd = 'python {}/index_setsm.py --np {} {} --mode tile --project arcticdem --write-json'.format(
            __app_dir__,
            os.path.join(self.tile_dir, 'v4', '59_57'),
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so, se) = p.communicate()
        # print(se)
        # print(so)

        json_list = [
            'arcticdem_59_57_2m.json',
        ]

        for json_fn in json_list:
            json = os.path.join(self.output_dir, json_fn)
            self.assertTrue(os.path.isfile(json))

        ## Test json read
        cmd = 'python {}/index_setsm.py --np {} {} --mode tile --project arcticdem --read-json'.format(
            __app_dir__,
            self.output_dir,
            self.test_str,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so, se) = p.communicate()
        # print(se)
        # print(so)

        self.assertTrue(os.path.isfile(self.test_str))
        ds = ogr.Open(self.test_str, 0)
        layer = ds.GetLayer()
        self.assertIsNotNone(layer)
        cnt = layer.GetFeatureCount()
        self.assertEqual(cnt, 4)
        ds, layer = None, None

    # @unittest.skip("test")
    def testTileJson_qtile(self):
        ## Test json creation
        cmd = 'python {}/index_setsm.py --np {} {} --mode tile --project arcticdem --write-json'.format(
            __app_dir__,
            os.path.join(self.tile_dir, 'v3', '33_11_quartertiles'),
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so, se) = p.communicate()
        # print(se)
        # print(so)

        json = os.path.join(self.output_dir, 'arcticdem_33_11_2m.json')
        self.assertTrue(os.path.isfile(json))

        ## Test json read
        cmd = 'python {}/index_setsm.py --np {} {} --mode tile --project arcticdem --read-json'.format(
            __app_dir__,
            self.output_dir,
            self.test_str,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so, se) = p.communicate()
        # print(se)
        # print(so)

        self.assertTrue(os.path.isfile(self.test_str))
        ds = ogr.Open(self.test_str, 0)
        layer = ds.GetLayer()
        self.assertIsNotNone(layer)
        cnt = layer.GetFeatureCount()
        self.assertEqual(cnt, 4)
        ds, layer = None, None


if __name__ == '__main__':

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Functional tests for index_setsm"
    )

    #### Parse Arguments
    args = parser.parse_args()

    test_cases = [
        TestIndexerScenes,
        TestIndexerStrips,
        TestIndexerTiles,
    ]

    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)

    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
