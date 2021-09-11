import os
import sys
import nltk
import numpy as np
from log import get_logger
from nltk.tokenize import word_tokenize
from nltk.tokenize import WordPunctTokenizer


class clean_text():
    """Clean audio data by removing dead spaces, ...
    """

    def __init__(self):
        self.logger = get_logger("clean_text")
    
    def loading(self, text_path):
        df=pd.read_csv(text_path,sep="\t",header=None)
        df = df.drop([0],axis=1)
        df.columns=['text']
        
        #clean data
        words_list=[' UNK ', ' music ', ' laughter ']
        df = df[~df['text'].isin(words_list)]
        for row in df['text']:
            for punctuation in string.punctuation:
                row = row.replace(punctuation," ")
        return df
    
    