import datetime
import luigi
import pandas as pd

from sqlalchemy import create_engine
from tasks.extract import *
from tasks.load import *
from tasks.transform import *
from utils.data_type import *


if __name__ == "__main__":
    luigi.configuration.get_config().lock_task_scheduler = False
    luigi.build([
        ExtractSalesData(), ExtractMarketingData(), ExtractArticles(),
        TransformSalesData(), TransformMarketingData(), TransformArticles(),
        LoadSalesData(), LoadMarketingData(), LoadArticles(),
    ], local_scheduler=True)
