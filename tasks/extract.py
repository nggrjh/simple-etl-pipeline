
import luigi
import pandas as pd

from sqlalchemy import create_engine


SOURCE_DATA_DIR = "data/source/"
EXTRACTED_DATA_DIR = "data/extracted/"


class ExtractSalesData(luigi.Task):

    def requires(self):
        pass

    def run(self):
        engine = create_engine(
            "postgresql://postgres:password123@localhost:5432/etl_db")
        extracted_data = pd.read_sql("SELECT * FROM amazon_sales_data", engine)
        extracted_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(EXTRACTED_DATA_DIR+"sales_data.csv")


class ExtractMarketingData(luigi.Task):

    def requires(self):
        pass

    def run(self):
        extracted_data = pd.read_csv(
            SOURCE_DATA_DIR+"ElectronicsProductsPricingData.csv")
        extracted_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(EXTRACTED_DATA_DIR+"marketing_data.csv")
