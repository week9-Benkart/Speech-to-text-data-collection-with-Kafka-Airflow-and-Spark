import numpy as np
import pandas as pd
from app_logger import App_Logger


app_logger = App_Logger("helper.log").get_app_logger()


class Helper:

    def __init__(self):
        self.logger = App_Logger("helper.log").get_app_logger()



    def save_csv(self, df, csv_path):
        try:
            df.to_csv(csv_path, index=False)
            print('File Successfully Saved.!!!')
            self.logger.info(f"File Successfully Saved to {csv_path}")

        except Exception:
            print("Save failed...")
            self.logger.error(f"saving failed")

        return df

  