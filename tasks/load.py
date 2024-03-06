import os
import pandas as pd

from tasks.transform import *
from utils.data_type import *
from sqlalchemy.dialects.postgresql import insert


class LoadSalesData(luigi.Task):

    def requires(self):
        return TransformSalesData()

    def run(self):
        transformed_data = pd.read_csv(self.input().path)
        os.remove(self.input().path)

        engine = create_engine(
            "postgresql://pacmann_dw:pacmann_dw@localhost:5433/data_warehouse")

        transformed_data.index += 1
        transformed_data.to_sql(
            name="dw_sales_data",
            con=engine,
            index=False,
            if_exists="append",
            method=self.insert_on_conflict_nothing,
        )

    def output(self):
        pass

    def insert_on_conflict_nothing(self, table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = insert(table.table).values(data)
        stmt = stmt.on_conflict_do_nothing(
            constraint="dw_sales_data_name_main_category_sub_category_key")
        result = conn.execute(stmt)
        return result.rowcount


class LoadMarketingData(luigi.Task):

    def requires(self):
        return TransformMarketingData()

    def run(self):
        transformed_data = pd.read_csv(self.input().path)
        os.remove(self.input().path)

        engine = create_engine(
            "postgresql://pacmann_dw:pacmann_dw@localhost:5433/data_warehouse")

        transformed_data.index += 1
        transformed_data.to_sql(
            name="dw_marketing_data",
            con=engine,
            index=False,
            if_exists="append",
        )

    def output(self):
        pass


class LoadArticles(luigi.Task):

    def requires(self):
        return TransformArticles()

    def run(self):
        transformed_data = pd.read_csv(self.input().path)
        os.remove(self.input().path)

        engine = create_engine(
            "postgresql://pacmann_dw:pacmann_dw@localhost:5433/data_warehouse")

        transformed_data.index += 1
        transformed_data.to_sql(
            name="dw_article_data",
            con=engine,
            index=False,
            if_exists="append",
            method=self.insert_on_conflict_nothing,
        )

    def output(self):
        pass

    def insert_on_conflict_nothing(self, table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = insert(table.table).values(data)
        stmt = stmt.on_conflict_do_nothing(
            constraint="dw_article_data_link_key")
        result = conn.execute(stmt)
        return result.rowcount
