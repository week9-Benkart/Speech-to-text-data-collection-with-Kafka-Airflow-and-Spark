import numpy as np
import pandas as pd
from app_logger import App_Logger


app_logger = App_Logger("helper.log").get_app_logger()


class Helper:
    ''' read and save csv files
    '''

    def __init__(self):
        self.logger = App_Logger("helper.log").get_app_logger()


    def read_csv(self, csv_path, missing_values=[]):
        try:
            df = pd.read_csv(csv_path, na_values=missing_values)
            print("file read as csv")
            self.logger.info(f"file read as csv from {csv_path}")
            return df
        except FileNotFoundError:
            print("file not found")
            self.logger.error(f"file not found, path:{csv_path}")

    def save_csv(self, df, csv_path):
        try:
            df.to_csv(csv_path, index=False)
            print('File Successfully Saved.!!!')
            self.logger.info(f"File Successfully Saved to {csv_path}")

        except Exception:
            print("Save failed...")
            self.logger.error(f"saving failed")

        return df

  