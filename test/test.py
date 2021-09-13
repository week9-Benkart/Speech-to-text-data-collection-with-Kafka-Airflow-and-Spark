import os
import sys
import unittest
import pandas as pd
import logging, logging.handlers

sys.path.append(os.path.abspath(os.path.join('../script')))
from df_helper import DfHelper


class TestDf(unittest.TestCase):
    ''' testing  functons in the helper class
    '''

    def setUp(self):
        self.helper = DfHelper()

    def test_to_csv(self):
        df = pd.DataFrame({'col1': range(1,4), 'col2': range(3,6)})
        self.helper.to_csv('../Data/data.csv', False)
        df2 = pd.read_csv('../Data/data.csv')


    def test_read_csv(self):
        df = self.helper.read_csv('../Data/test.csv')
        df2 = pd.read_csv('../Data/test.csv')
      
    if __name__ == '__main__':
        unittest.main()