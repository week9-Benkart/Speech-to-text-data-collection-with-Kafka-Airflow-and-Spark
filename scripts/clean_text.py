import os
import sys
import nltk
import numpy as np
import pandas as pd
from app_logger import App_Logger
from nltk.tokenize import word_tokenize
from nltk.tokenize import WordPunctTokenizer


class clean_text():
    """Clean audio data by removing dead spaces, ...
    """

    def __init__(self):
        self.logger = App_Logger().get_logger(__name__)

    def get_text(self, filters):
        DIRECTORY = '../data/preprocessed'
        amharic = []
        sub_dir = [DIRECTORY + '/' + i for i in os.listdir(DIRECTORY)]
        for d in sub_dir:
            with open(d) as fp:
                line = fp.readline()
                while line:
                    sent = ''.join( c for c in line if  c not in filters)
                    amharic.append( sent.rstrip().strip())
                    line = fp.readline()

        return amharic

        filters = '!"#$%&()*+,-/:;<=>?@[\\]^_`{|}~\t\n።”፤፦’፥፣.“‘·\'—\t\n'
        amharic = get_text(filters)
        print("num amharic sentences: ", len(amharic))