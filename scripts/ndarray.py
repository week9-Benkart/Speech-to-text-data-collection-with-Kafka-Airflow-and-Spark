import os
import sys
sys.path.append(os.path.abspath(os.path.join('../scripts')))
import pandas as pd
import numpy as np
import logging
import boto3
from botocore.exceptions import ClientError
import glob
from scipy.io.wavfile import read
from app_logger import App_Logger

class wav_to_ndarray_to_s3():
    """converting wav file to ndarray and saving in s3, ...
    """

    def __init__(self):
        self.logger = get_logger("wav_to_ndarray_to_s3")


    def load_data(dataset_path):
        """ Loads sample data from path and return lists of paths 
        :param dataset_path: Files to be loaded
        :return: list of the paths
        """
        print("Loading the audio files")
        labels=[]
        # dictionary to store files

        # loop through all sub-folders
        for i, (dirpath, dirnames, filenames) in enumerate(os.walk(dataset_path)):

            # ensure we're processing at sub-folder level
            if dirpath is not dataset_path:

                # save label (i.e., sub-folder name) in the mapping 
                label = dirpath#.split("/")[-1]

                # process all audio files in the sub-directory
                for f in filenames:

                    # load audio file
                    filename=label+"/"+f
                    labels.append(filename)
        return labels


    def convert_to_ndarray(DATASET_PATH):
        """converts all the wav file into ndarray and appends in a list
        :param DATASET_PATH: a path to wav files
        :return: list containg tuples of ndarrays
        """
        wavs = []
        for filename in glob.glob(DATASET_PATH + "wav/*.wav"):
            wavs.append(read(filename))
        return wavs


#     def to_csv():
#         d = {'PATH':PATH,'NDARRAY':wavs}
#         df = pd.DataFrame(d)
#         df.to_csv('ndarray.csv')

    
    def upload_ndarray(file_name: str, bucket: str, object_name=None):
        """Upload a file to an S3 bucket
        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True