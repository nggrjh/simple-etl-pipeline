import datetime
import pandas as pd

from tasks.transform import *
from utils.data_type import *


class LoadSalesData(luigi.Task):

    def requires(self):
        return TransformSalesData()

    def run(self):
        transformed_data = pd.read_csv(self.input().path)
        transformed_data["created_at"] = datetime.datetime.now()

        engine = create_engine(
            'postgresql://pacmann_dw:pacmann_dw@localhost:5433/data_warehouse')

        transformed_data.index += 1
        transformed_data.to_sql(
            name='dw_sales_data',
            con=engine,
            index=True,
            index_label='id',
            if_exists='replace',
        )

    def output(self):
        pass
