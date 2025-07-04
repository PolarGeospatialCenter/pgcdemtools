import argparse
import os
import sys
import unittest

__test_dir__ = os.path.dirname(__file__)
__app_dir__ = os.path.dirname(__test_dir__)
sys.path.append(__app_dir__)

from lib import taskhandler


class TestConvertArgs(unittest.TestCase):
    
    def setUp(self):
        self.args = Args()
    
    def test_args(self):
        positional_arg_keys = ['positional']
        arg_keys_to_remove = ['toremove', 'to_remove']
        arg_str = taskhandler.convert_optional_args_to_string(self.args, positional_arg_keys, arg_keys_to_remove)
        self.assertIn('--tuple item1 item2', arg_str)
        self.assertIn('--list item1 item2', arg_str)
        self.assertIn('--boolean', arg_str)
        self.assertIn('--multi-word-key multi-word-key', arg_str)
        self.assertNotIn('positional', arg_str)
        self.assertNotIn('toremove', arg_str)
        self.assertNotIn('to-remove', arg_str)
        self.assertNotIn('to_remove', arg_str)
        
        
class Args(object):
    def __init__(self):
        self.boolean = True
        self.multi_word_key = "multi-word-key"
        self.list = ['item1', 'item2']
        self.tuple = ('item1', 'item2')
        self.positional = 'positional'
        self.toremove = 'removed'
        self.to_remove = 'removed'
        

if __name__ == '__main__':
    
    #### Set Up Arguments
    parser = argparse.ArgumentParser(
        description="Test taskhandler package"
        )

    #### Parse Arguments
    args = parser.parse_args()
        
    test_cases = [
        TestConvertArgs,
    ]
    
    suites = []
    for test_case in test_cases:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
        suites.append(suite)
    
    alltests = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=2).run(alltests)
