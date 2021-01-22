import unittest, os, sys, glob, shutil, argparse, logging, subprocess, ConfigParser
import gdal, ogr, osr, gdalconst

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
testdata_dir = os.path.join(script_dir,'testdata')
root_dir = os.path.dirname(script_dir)
sys.path.append(root_dir)

res_str = {
    2.0: '_2m_v',
    0.5: '_50cm_v',
}

# logger = logging.getLogger("logger")
# lso = logging.StreamHandler()
# lso.setLevel(logging.ERROR)
# formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
# lso.setFormatter(formatter)
# logger.addHandler(lso)

class TestIndexerIO(unittest.TestCase):

    def setUp(self):
        self.scene_dir = os.path.join(testdata_dir,'setsm_scene')
        self.scene50cm_dir = os.path.join(testdata_dir,'setsm_scene_50cm')
        self.scenedsp_dir = os.path.join(testdata_dir, 'setsm_scene_2mdsp')
        self.strip_dir = os.path.join(testdata_dir,'setsm_strip')
        self.stripmasked_dir = os.path.join(testdata_dir,'setsm_strip_masked')
        self.tile_dir = os.path.join(testdata_dir,'setsm_tile')
        self.output_dir = os.path.join(testdata_dir, 'output')
        self.test_str = os.path.join(self.output_dir, 'test.shp')
        self.pg_test_str = 'PG:sandwich:test'

        self.scene_count = 51
        self.scene50cm_count = 14
        self.scenedsp_count = 102
        self.strip_count = 4
        self.stripmasked_count = 3

    def tearDown(self):
        ## Clean up output
        for f in os.listdir(self.output_dir):
            fp = os.path.join(self.output_dir,f)
            if os.path.isfile(fp):
                os.remove(fp)
            else:
                shutil.rmtree(fp)

    #@unittest.skip("test")
    def testOutputShp(self):

        ## Build shp
        test_param_list = (
            # input, output, args, result feature count, message
            (self.scene_dir, self.test_str, '', self.scene_count, 'Done'),  # test creation
            (self.scene_dir, self.test_str, '--append', self.scene_count * 2, 'Done'),  # test append
            (self.scene_dir, self.test_str, '', self.scene_count * 2, 'Dst shapefile exists.  Use the --overwrite or --append options.'), # test error meeasge on existing
            (self.scene_dir, self.test_str, '--overwrite --check', self.scene_count, 'Removing old index'), # test overwrite
        )

        for i, o, options, result_cnt, msg in test_param_list:
            cmd = 'python index_setsm.py {} {} --skip-region-lookup {}'.format(
                i,
                o,
                options
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so,se) = p.communicate()
            # print(se)
            # print(so)

            ## Test if ds exists and has correct number of records
            self.assertTrue(os.path.isfile(o))
            ds = ogr.Open(o,0)
            layer = ds.GetLayer()
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt,result_cnt)
            for feat in layer:
                srcfn = os.path.basename(feat.GetField('LOCATION'))
                is_xtrack = 0 if srcfn.startswith(('WV','GE','QB')) else 1
                self.assertEqual(feat.GetField('IS_XTRACK'),is_xtrack)
            ds, layer = None, None

            ##Test if stdout has proper error
            self.assertIn(msg,(se))

    def testOutputGdb(self):

        self.test_str = os.path.join(self.output_dir, 'test.gdb', 'test_lyr')

        ## Build shp
        test_param_list = (
            # input, output, args, result feature count, message
            (self.scene_dir, self.test_str, '', self.scene_count, 'Done'),  # test creation
            (self.scene_dir, self.test_str, '--append', self.scene_count * 2 , 'Done'),  # test append
            (self.scene_dir, self.test_str, '', self.scene_count * 2, 'Dst GDB layer exists.  Use the --overwrite or --append options.'), # test error meeasge on existing
            (self.scene_dir, self.test_str, '--overwrite --check', self.scene_count, 'Removing old index'), # test overwrite
        )

        for i, o, options, result_cnt, msg in test_param_list:
            cmd = 'python index_setsm.py {} {} --skip-region-lookup {}'.format(
                i,
                o,
                options
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so,se) = p.communicate()
            # print(se)
            # print(so)

            ## Test if ds exists and has corrent number of records
            gdb,lyr = os.path.split(o)
            self.assertTrue(os.path.isdir(gdb))
            ds = ogr.Open(gdb,0)
            layer = ds.GetLayerByName(lyr)
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt,result_cnt)
            ds, layer = None, None

            ##Test if stdout has proper error
            self.assertIn(msg,(se))

    def testOutputPostgres(self):

        ## Get config info
        protocol,section,lyr = self.pg_test_str.split(':')
        try:
            config = ConfigParser.SafeConfigParser()
        except NameError:
            config = ConfigParser.ConfigParser()  # ConfigParser() replaces SafeConfigParser() in Python >=3.2
        config.read(os.path.join(root_dir,'config.ini'))
        conn_info = {
            'host':config.get(section,'host'),
            'port':config.getint(section,'port'),
            'name':config.get(section,'name'),
            'schema':config.get(section,'schema'),
            'user':config.get(section,'user'),
            'pw':config.get(section,'pw'),
        }
        pg_conn_str = "PG:host={host} port={port} dbname={name} user={user} password={pw} active_schema={schema}".format(**conn_info)

        ## Build shp
        test_param_list = (
            # input, output, args, result feature count, message
            (self.scene_dir, self.pg_test_str, '', self.scene_count, 'Done', 2),  # test creation
            (self.scene_dir, self.pg_test_str, '--append', self.scene_count * 2, 'Done', 2),  # test append
            (self.scene_dir, self.pg_test_str, '', self.scene_count * 2, 'Dst DB layer exists.  Use the --overwrite or --append options.', 2), # test error meeasge on existing
            (self.scene_dir, self.pg_test_str, '--overwrite --check', self.scene_count, 'Removing old index', 2), # test overwrite
            (self.scenedsp_dir, self.pg_test_str, '--overwrite', self.scenedsp_count, 'Done', 2),  # test as 2m_dsp record
            (self.scenedsp_dir, self.pg_test_str, '--overwrite --dsp-original-res', self.scenedsp_count, 'Done', 0.5),
        )

        ## Ensure test layer does not exist on DB
        ds = ogr.Open(pg_conn_str,1)
        for i in range(ds.GetLayerCount()):
            l = ds.GetLayer(i)
            if l.GetName() == lyr:
                ds.DeleteLayer(i)
                break

        for i, o, options, result_cnt, msg, res in test_param_list:
            cmd = 'python index_setsm.py {} {} --skip-region-lookup {}'.format(
                i,
                o,
                options
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so,se) = p.communicate()
            # print(se)
            # print(so)

            ## Test if ds exists and has corrent number of records
            ds = ogr.Open(pg_conn_str,0)
            layer = ds.GetLayerByName(lyr)
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt,result_cnt)
            for feat in layer:
                scenedemid = feat.GetField('SCENEDEMID')
                stripdemid = feat.GetField('STRIPDEMID')
                self.assertEqual(feat.GetField('DEM_RES'), res)
                scenedemid_lastpart = scenedemid.split('_')[-1]
                self.assertTrue(scenedemid_lastpart.startswith('2' if res == 2.0 else '0'))
                self.assertTrue(res_str[res] in stripdemid)
            ds, layer = None, None

            ##Test if stdout has proper error
            self.assertIn(msg,(se))

        ## Ensure test layer does not exist on DB
        ds = ogr.Open(pg_conn_str,1)
        for i in range(ds.GetLayerCount()):
            l = ds.GetLayer(i)
            if l.GetName() == lyr:
                ds.DeleteLayer(i)
                break

    def testScene50cm(self):

        ## Build shp
        test_param_list = (
            # input, output, args, result feature count, message
            (self.scene50cm_dir, self.test_str, '', self.scene50cm_count, 'Done'),  # test creation
        )

        for i, o, options, result_cnt, msg in test_param_list:
            cmd = 'python index_setsm.py {} {} --skip-region-lookup {}'.format(
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

            ##Test if stdout has proper error
            self.assertIn(msg, (se))

    def testSceneDsp(self):

        ## Build shp
        test_param_list = (
            # input, output, args, result feature count, message
            (self.scenedsp_dir, self.test_str, '', self.scenedsp_count, 'Done', 2),  # test as 2m_dsp record
            (self.scenedsp_dir, self.test_str, '--overwrite --dsp-original-res --check', self.scenedsp_count, 'Done', 0.5),  # test as 50cm record
        # test as 50cm record
        )

        for i, o, options, result_cnt, msg, res in test_param_list:
            cmd = 'python index_setsm.py {} {} --skip-region-lookup {}'.format(
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
                self.assertEqual(feat.GetField('DEM_RES'),res)
                scenedemid_lastpart = scenedemid.split('_')[-1]
                location_lastpart = location.split('_')[-2]
                self.assertTrue(scenedemid_lastpart.startswith('2' if res == 2.0 else '0'))
                if '-' in location_lastpart:
                    self.assertEqual(scenedemid_lastpart.split('-')[1], location_lastpart.split('-')[1])
                self.assertTrue(res_str[res] in stripdemid)
                self.assertEqual(feat.GetField('IS_DSP'), 1 if res == 2.0 else 0)
            ds, layer = None, None

            # Test if stdout has proper error
            self.assertIn(msg, (se))

    def testSceneJson(self):

        ## Test json creation
        cmd = 'python index_setsm.py {} {} --write-json'.format(
            self.scene_dir,
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        # print(se)
        # print(so)

        jsons = [
            os.path.join(self.output_dir, 'WV02_20190419_103001008C4B0400_103001008EC59A00_2m_v040002.json'),
            os.path.join(self.output_dir, 'WV02_20190705_103001009505B700_10300100934D1000_2m_v040002.json'),
            os.path.join(self.output_dir, 'W1W1_20190426_102001008466F300_1020010089C2DB00_2m_v030403.json'),
        ]

        counter = 0
        for json in jsons:
            self.assertTrue(os.path.isfile(json))
            fh = open(json)
            for line in fh:
                cnt = line.count('sceneid')
                counter += cnt
        self.assertEqual(counter,self.scene_count)

        ## Test json exists error
        msg = 'Json file already exists'
        cmd = 'python index_setsm.py {} {} --write-json'.format(
            self.scene_dir,
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        # print(se)
        # print(so)

        self.assertIn(msg,se)

        ## Test json overwrite
        stat = os.stat(os.path.join(self.output_dir,'WV02_20190419_103001008C4B0400_103001008EC59A00_2m_v040002.json'))
        mod_date1 = stat.st_mtime

        cmd = 'python index_setsm.py {} {} --write-json --overwrite'.format(
            self.scene_dir,
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        # print(se)
        # print(so)

        stat = os.stat(os.path.join(self.output_dir,'WV02_20190419_103001008C4B0400_103001008EC59A00_2m_v040002.json'))
        mod_date2 = stat.st_mtime
        self.assertGreater(mod_date2,mod_date1)

        ## Test json read
        test_shp = os.path.join(self.output_dir,'test.shp')
        cmd = 'python index_setsm.py {} {} --skip-region-lookup --read-json --check'.format(
            self.output_dir,
            test_shp,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        # print(se)
        # print(so)

        self.assertTrue(os.path.isfile(test_shp))
        ds = ogr.Open(test_shp,0)
        layer = ds.GetLayer()
        self.assertIsNotNone(layer)
        cnt = layer.GetFeatureCount()
        self.assertEqual(cnt,self.scene_count)
        ds, layer = None, None

    def testSceneDspJson(self):

        test_param_list = (
            ('', 2.0),
            ('--dsp-original-res --overwrite', 0.5),
        )

        ## Test json creation
        cmd = 'python index_setsm.py {} {} --write-json'.format(
            self.scenedsp_dir,
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        # print(se)
        # print(so)

        json1 = os.path.join(self.output_dir,'WV01_20120317_10200100192B8400_102001001AC4FE00_2m_v040201.json')
        self.assertTrue(os.path.isfile(json1))

        ## Test json read
        test_shp = os.path.join(self.output_dir,'test.shp')
        for options, res in test_param_list:
            cmd = 'python index_setsm.py {} {} {} --skip-region-lookup --read-json'.format(
                self.output_dir,
                test_shp,
                options,
            )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        # print(se)
        # print(so)

        self.assertTrue(os.path.isfile(test_shp))
        ds = ogr.Open(test_shp,0)
        layer = ds.GetLayer()
        self.assertIsNotNone(layer)
        cnt = layer.GetFeatureCount()
        self.assertEqual(cnt,self.scenedsp_count)
        feat = layer.GetFeature(1)
        scenedemid = feat.GetField('SCENEDEMID')
        stripdemid = feat.GetField('STRIPDEMID')
        self.assertEqual(feat.GetField('DEM_RES'), res)
        scenedemid_lastpart = scenedemid.split('_')[-1]
        self.assertTrue(scenedemid_lastpart.startswith('2' if res == 2.0 else '0'))
        self.assertTrue(res_str[res] in stripdemid)
        self.assertEqual(feat.GetField('IS_DSP'), 1 if res == 2.0 else 0)
        ds, layer = None, None

    def testStrip(self):

        test_param_list = (
            # input, output, args, result feature count, message
            (self.strip_dir, self.test_str, '', self.strip_count, 'Done'),  # test creation
            (self.stripmasked_dir, self.test_str, '--overwrite --check', self.stripmasked_count, 'Done'),  # test index of masked strips
            (self.stripmasked_dir, self.test_str, '--overwrite --search-masked', self.stripmasked_count * 5, 'Done'),  # test index of masked strips
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
            cmd = 'python index_setsm.py --mode strip {} {} --skip-region-lookup {}'.format(
                i,
                o,
                options
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so,se) = p.communicate()
            # print(se)
            # print(so)

            ## Test if ds exists and has correct number of records
            self.assertTrue(os.path.isfile(o))
            ds = ogr.Open(o,0)
            layer = ds.GetLayer()
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt,result_cnt)
            for feat in layer:
                srcfn = os.path.basename(feat.GetField('LOCATION'))
                dem_suffix = srcfn[srcfn.find('_dem'):]
                masks = strip_masks[dem_suffix]
                self.assertEqual(feat.GetField('EDGEMASK'), masks[0])
                self.assertEqual(feat.GetField('WATERMASK'), masks[1])
                self.assertEqual(feat.GetField('CLOUDMASK'), masks[2])
                is_xtrack = 0 if srcfn.startswith(('WV','GE','QB')) else 1
                self.assertEqual(feat.GetField('IS_XTRACK'),is_xtrack)
            ds, layer = None, None

            ## Test if stdout has proper error
            self.assertIn(msg,se)

    def testStripJson(self):
        ## Test json creation
        cmd = 'python index_setsm.py {} {} --mode strip --write-json'.format(
            self.strip_dir,
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        # print(se)
        # print(so)

        json_list = (
            'WV01_20140402_102001002C6AFA00_102001002D8B3100_2m_v030202.json',
        )

        for json_fn in json_list:
            json = os.path.join(self.output_dir,json_fn)
            self.assertTrue(os.path.isfile(json))

        ## Test json read
        cmd = 'python index_setsm.py {} {} --mode strip --skip-region-lookup --read-json'.format(
            self.output_dir,
            self.test_str,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        # print(se)
        # print(so)

        self.assertTrue(os.path.isfile(self.test_str))
        ds = ogr.Open(self.test_str,0)
        layer = ds.GetLayer()
        self.assertIsNotNone(layer)
        cnt = layer.GetFeatureCount()
        self.assertEqual(cnt,self.strip_count)
        ds, layer = None, None

    def testTile(self):

        test_param_list = (
            # input, output, args, result feature count, message
            (os.path.join(self.tile_dir,'v3','33_11'), self.test_str, '--project arcticdem', 3, 'Done'),  # test 100x100km tile at 3 resolutions
            (os.path.join(self.tile_dir,'v3','33_11_quartertiles'), self.test_str, '--overwrite --project arcticdem', 4, 'Done'), # test quartertiles formatted for release
            (os.path.join(self.tile_dir,'v4','59_57'), self.test_str, '--overwrite --check --project arcticdem', 4, 'Done'),  # test v4 tiles, 2m
            (os.path.join(self.tile_dir, 'v4', 'utm34n_60_06'), self.test_str, '--overwrite --project earthdem', 4, 'Done'),  # test v4 utm tiles, 2m
        )

        for i, o, options, result_cnt, msg in test_param_list:
            cmd = 'python index_setsm.py --mode tile  {} {} {}'.format(
                i,
                o,
                options
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so,se) = p.communicate()
            # print(se)
            # print(so)

            ## Test if ds exists and has correct number of records
            self.assertTrue(os.path.isfile(o))
            ds = ogr.Open(o,0)
            layer = ds.GetLayer()
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt,result_cnt)
            ds, layer = None, None

            ## Test if stdout has proper error
            self.assertIn(msg,se)

    def testTileJson(self):
        ## Test json creation
        cmd = 'python index_setsm.py {} {} --mode tile --project arcticdem --write-json'.format(
            os.path.join(self.tile_dir,'v3','33_11'),
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        #print(se)
        #print(so)

        json_list = [
            'arcticdem_33_11_2m.json',
            'arcticdem_33_11_40m.json',
            'arcticdem_33_11_10m.json',
        ]

        for json_fn in json_list:
            json = os.path.join(self.output_dir,json_fn)
            self.assertTrue(os.path.isfile(json))

        ## Test json read
        cmd = 'python index_setsm.py {} {} --mode tile --project arcticdem --read-json'.format(
            self.output_dir,
            self.test_str,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        # print(se)
        # print(so)

        self.assertTrue(os.path.isfile(self.test_str))
        ds = ogr.Open(self.test_str,0)
        layer = ds.GetLayer()
        self.assertIsNotNone(layer)
        cnt = layer.GetFeatureCount()
        self.assertEqual(cnt,3)
        ds, layer = None, None

    def testTilev4Json(self):
        ## Test json creation
        cmd = 'python index_setsm.py {} {} --mode tile --project arcticdem --write-json'.format(
            os.path.join(self.tile_dir,'v4','59_57'),
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        #print(se)
        #print(so)

        json_list = [
            'arcticdem_59_57_2m.json',
        ]

        for json_fn in json_list:
            json = os.path.join(self.output_dir,json_fn)
            self.assertTrue(os.path.isfile(json))

        ## Test json read
        cmd = 'python index_setsm.py {} {} --mode tile --project arcticdem --read-json'.format(
            self.output_dir,
            self.test_str,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        # print(se)
        # print(so)

        self.assertTrue(os.path.isfile(self.test_str))
        ds = ogr.Open(self.test_str,0)
        layer = ds.GetLayer()
        self.assertIsNotNone(layer)
        cnt = layer.GetFeatureCount()
        self.assertEqual(cnt,4)
        ds, layer = None, None

    def testTileJson_qtile(self):
        ## Test json creation
        cmd = 'python index_setsm.py {} {} --mode tile --project arcticdem --write-json'.format(
            os.path.join(self.tile_dir,'v3','33_11_quartertiles'),
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        #print(se)
        #print(so)

        json = os.path.join(self.output_dir,'arcticdem_33_11_2m.json')
        self.assertTrue(os.path.isfile(json))

        ## Test json read
        cmd = 'python index_setsm.py {} {} --mode tile --project arcticdem --read-json'.format(
            self.output_dir,
            self.test_str,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        # print(se)
        # print(so)

        self.assertTrue(os.path.isfile(self.test_str))
        ds = ogr.Open(self.test_str,0)
        layer = ds.GetLayer()
        self.assertIsNotNone(layer)
        cnt = layer.GetFeatureCount()
        self.assertEqual(cnt,4)
        ds, layer = None, None


## test custom path behavior
## test region lookup
## test bad config file


if __name__ == '__main__':

    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Functional test for index_setsm"
        )

    #### Parse Arguments
    args = parser.parse_args()

    test_cases = [
        TestIndexerIO,
    ]

    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)

    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)