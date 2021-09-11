import boto3
import pandas as pd

def load_data_s3(bucket_name='10academy-group2-bucket',file_name='Clean_Amharic.txt'):
    """ Load transcription data from s3 bucket"""
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1'
    )
    # Load file directly into python
    obj = s3.Bucket(bucket_name).Object(file_name).get()
    df = pd.read_csv(obj['Body'])
    return df

