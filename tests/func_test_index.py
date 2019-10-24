import unittest, os, sys, glob, shutil, argparse, logging, subprocess, ConfigParser
import gdal, ogr, osr, gdalconst

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
root_dir = os.path.dirname(script_dir)
sys.path.append(root_dir)

# logger = logging.getLogger("logger")
# lso = logging.StreamHandler()
# lso.setLevel(logging.ERROR)
# formatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s','%m-%d-%Y %H:%M:%S')
# lso.setFormatter(formatter)
# logger.addHandler(lso)

class TestIndexerIO(unittest.TestCase):

    def setUp(self):
        self.scene_dir = os.path.join(script_dir,'testdata','setsm_scene')
        self.scene_json = os.path.join(script_dir,'testdata','setsm_scene','json')
        self.strip_dir = os.path.join(script_dir,'testdata','setsm_strip')
        self.tile_dir = os.path.join(script_dir,'testdata','setsm_tile')
        self.output_dir = os.path.join(script_dir, 'testdata', 'output')
        self.test_str = os.path.join(self.output_dir, 'test.shp')


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
            (self.scene_dir, self.test_str, '', 43, 'Done'),  # test creation
            (self.scene_dir, self.test_str, '--append', 86, 'Done'),  # test append
            (self.scene_dir, self.test_str, '', 86, 'Dst shapefile exists.  Use the --overwrite flag to overwrite.'), # test error meeasge on existing
            (self.scene_dir, self.test_str, '--overwrite', 43, 'Removing old index'), # test overwrite
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
            self.assertTrue(os.path.isfile(o))
            ds = ogr.Open(o,0)
            layer = ds.GetLayer()
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt,result_cnt)
            ds, layer = None, None

            ##Test if stdout has proper error
            self.assertIn(msg,(se))

    #@unittest.skip("test")
    def testOutputGdb(self):

        self.test_str = os.path.join(self.output_dir, 'test.gdb', 'test_lyr')

        ## Build shp
        test_param_list = (
            # input, output, args, result feature count, message
            (self.scene_dir, self.test_str, '', 43, 'Done'),  # test creation
            (self.scene_dir, self.test_str, '--append', 86, 'Done'),  # test append
            (self.scene_dir, self.test_str, '', 86, 'Dst GDB layer exists.  Use the --overwrite flag to overwrite.'), # test error meeasge on existing
            (self.scene_dir, self.test_str, '--overwrite', 43, 'Removing old index'), # test overwrite
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

    #@unittest.skip("test")
    def testOutputPostgres(self):

        ## Get config info
        self.test_str = 'PG:sandwich:test'
        protocol,section,lyr = self.test_str.split(':')
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
            (self.scene_dir, self.test_str, '', 43, 'Done'),  # test creation
            (self.scene_dir, self.test_str, '--append', 86, 'Done'),  # test append
            (self.scene_dir, self.test_str, '', 86, 'Dst DB layer exists.  Use the --overwrite flag to overwrite.'), # test error meeasge on existing
            (self.scene_dir, self.test_str, '--overwrite', 43, 'Removing old index'), # test overwrite
        )

        ## Ensure test layer does not exist on DB
        ds = ogr.Open(pg_conn_str,0)
        for i in range(ds.GetLayerCount()):
            l = ds.GetLayer(i)
            if l.GetName() == lyr:
                ds.DeleteLayer(i)

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
            ds = ogr.Open(pg_conn_str,0)
            layer = ds.GetLayerByName(lyr)
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt,result_cnt)
            ds, layer = None, None

            ##Test if stdout has proper error
            self.assertIn(msg,(se))

        ## Ensure test layer does not exist on DB
        ds = ogr.Open(pg_conn_str,0)
        for i in range(ds.GetLayerCount()):
            l = ds.GetLayer(i)
            if l.GetName() == lyr:
                ds.DeleteLayer(i)

    #@unittest.skip("test")
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

        json1 = os.path.join(self.output_dir,'WV02_20190419_103001008C4B0400_103001008EC59A00_2m_v402.json')
        json2 = os.path.join(self.output_dir,'WV02_20190705_103001009505B700_10300100934D1000_2m_v402.json')
        self.assertTrue(os.path.isfile(json1))
        self.assertTrue(os.path.isfile(json2))

        counter = 0
        for json in json1,json2:
            fh = open(json)
            for line in fh:
                cnt = line.count('sceneid')
                counter += cnt
        self.assertEqual(counter,43)

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
        stat = os.stat(os.path.join(self.output_dir,'WV02_20190419_103001008C4B0400_103001008EC59A00_2m_v402.json'))
        mod_date1 = stat.st_mtime

        cmd = 'python index_setsm.py {} {} --write-json --overwrite'.format(
            self.scene_dir,
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        # print(se)
        # print(so)

        stat = os.stat(os.path.join(self.output_dir,'WV02_20190419_103001008C4B0400_103001008EC59A00_2m_v402.json'))
        mod_date2 = stat.st_mtime
        self.assertGreater(mod_date2,mod_date1)

        ## Test json read
        test_shp = os.path.join(self.output_dir,'test.shp')
        cmd = 'python index_setsm.py {} {} --skip-region-lookup --read-json'.format(
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
        self.assertEqual(cnt,43)
        ds, layer = None, None


    def testStrip(self):

        test_param_list = (
            # input, output, args, result feature count, message
            (self.strip_dir, self.test_str, '', 20, 'Done'),  # test creation
        )

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

            ## Test if ds exists and has corrent number of records
            self.assertTrue(os.path.isfile(o))
            ds = ogr.Open(o,0)
            layer = ds.GetLayer()
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt,result_cnt)
            ds, layer = None, None

            ##Test if stdout has proper error
            self.assertIn(msg,se)


    def testStripJson(self):
        ## Test json creation
        cmd = 'python index_setsm.py {} {} --mode strip --write-json'.format(
            self.strip_dir,
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        #print(se)
        #print(so)

        json_list = (
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg1.json',
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg2.json',
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg3.json',
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg4.json',
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg5.json',
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg6.json',
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg7.json',
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg8.json',
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg9.json',
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg10.json',
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg11.json',
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg12.json',
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg13.json',
            'W1W1_20080124_1020010001577B00_1020010001B3EF00_2m_lsf_seg14.json',
            'WV01_20101005_102001000EB73400_102001000F994F00_seg1_2m.json',
            'WV01_20181130_102001007D6AD000_102001007EAD9100_2m_lsf_seg1.json',
            'WV01_20181130_102001007D6AD000_102001007EAD9100_2m_lsf_seg2.json',
            'WV01_20181130_102001007D6AD000_102001007EAD9100_2m_lsf_seg3.json',
            'WV01_20181130_102001007D6AD000_102001007EAD9100_2m_lsf_seg4.json',
            'WV02_20180401_103001007B43EF00_103001007A40F400_seg1_2m.json'
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
        self.assertEqual(cnt,20)
        ds, layer = None, None


    def testTile(self):

        test_param_list = (
            # input, output, args, result feature count, message
            (os.path.join(self.tile_dir,'tile'), self.test_str, '', 1, 'Done'),  # test 100x100km tile
            (os.path.join(self.tile_dir,'rel_tile'), self.test_str, '--overwrite', 4, 'Done'), # test quartertiles formatted for release
        )

        for i, o, options, result_cnt, msg in test_param_list:
            cmd = 'python index_setsm.py --mode tile --project arcticdem {} {} {}'.format(
                i,
                o,
                options
            )
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (so,se) = p.communicate()
            #print(se)
            #print(so)

            ## Test if ds exists and has corrent number of records
            self.assertTrue(os.path.isfile(o))
            ds = ogr.Open(o,0)
            layer = ds.GetLayer()
            self.assertIsNotNone(layer)
            cnt = layer.GetFeatureCount()
            self.assertEqual(cnt,result_cnt)
            ds, layer = None, None

            ##Test if stdout has proper error
            self.assertIn(msg,se)


    def testTileJson(self):
        ## Test json creation
        cmd = 'python index_setsm.py {} {} --mode tile --project arcticdem --write-json'.format(
            os.path.join(self.tile_dir,'tile'),
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        #print(se)
        #print(so)

        json = os.path.join(self.output_dir,'arcticdem_14_29_2m.json')
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
        self.assertEqual(cnt,1)
        ds, layer = None, None


    def testTileJson_qtile(self):
        ## Test json creation
        cmd = 'python index_setsm.py {} {} --mode tile --project arcticdem --write-json'.format(
            os.path.join(self.tile_dir,'rel_tile'),
            self.output_dir,
        )
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (so,se) = p.communicate()
        #print(se)
        #print(so)

        json = os.path.join(self.output_dir,'arcticdem_14_29_2m.json')
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


## test attribute contents
## test db_path_prefix behavior
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