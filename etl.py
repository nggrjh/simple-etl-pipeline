import datetime
import luigi
import pandas as pd

from sqlalchemy import create_engine
from tasks.extract import *
from tasks.load import *
from tasks.transform import *
from utils.data_type import *


if __name__ == "__main__":
    luigi.build([
        ExtractSalesData(), TransformSalesData(), LoadSalesData(),
    ], local_scheduler=True)
