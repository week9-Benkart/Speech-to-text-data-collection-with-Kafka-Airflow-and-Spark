import boto3
import pandas as pd
import scipy.io.wavfile as wav
import io

class ReadFromS3:
    def __init__(self,bucket_name='10academy-group2-bucket',file_type="text"):
        self.bucket_name = bucket_name
        self.file_type = file_type
        self.files = {}
    
    def return_files(self):
        """ Return an iterable data from s3 bucket """
        self.initialize_bucket()
        self.load_files()
        if self.file_type == "text":
            return self.read_text_files()
        elif self.file_type == "audio":
            return self.read_audio_files()
        else:
            return "error"
        
    def initialize_bucket(self):
        """ Initialize s3 bucket"""
        s3 = boto3.resource(
            service_name='s3',
            region_name='us-east-1'
        )
        self.bucket = s3.Bucket(self.bucket_name)
    
    def get_obj(self,file_name):
        self.bucket.Object(file_name).get()
        return obj['Body'].read()
        
    
    def load_files(self):
        """ Iterate """
        audio_files = []
        byte_files = []
        for obj in self.bucket.objects.all():
            key = obj.key
            body = obj.get()['Body'].read()
            audio_files.append(key)
            byte_files.append(body)
        self.files["key"] = audio_files
        self.files["body_in_bytes"] = byte_files
            
    def read_text_files(self):
        """ Iterate through the byte files and convert to pandas df 
            Code adapted from https://www.sqlservercentral.com/articles/
            reading-a-specific-file-from-an-s3-bucket-using-python """
        df = pd.DataFrame()
        df_list = []
        for file in self.files["body_in_bytes"]:
            df_list.append(pd.read_csv(io.BytesIO(file), header=0, delimiter=","))
        df = pd.concat(df_list, ignore_index=True)
        df.columns = ["sentence"]
    
        return df
    
    def read_audio_files(self):
        """ Iterate through the key files and convert to df"""
        df = pd.DataFrame({"file_name": self.files["key"],"audio_file": self.files["body_in_bytes"]})
        return df
    

        

