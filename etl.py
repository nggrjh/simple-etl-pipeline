import luigi
import pandas as pd
import datetime

from sqlalchemy import create_engine

EXTRACTED_DATA_DIR = "data/extracted/"
TRANSFORMED_DATA_DIR = "data/transformed/"


def set_float_column(df, col):
    df[col] = df[col].str.replace(',', '').replace('', '0')
    df.loc[df[col].str.contains(r'[^0-9.]', regex=True), col] = '0'
    df[col] = df[col].astype(float)
    return df


def set_rating_column(df, col):
    df[col] = df[col].fillna('')
    return set_float_column(df, col)


def set_money_column(df, col):
    df[col] = df[col].fillna('').str.replace('₹', '')
    return set_float_column(df, col)


def replace_column(df, col, map):
    df[col] = df[col].replace(map)
    return df


class ExtractSalesData(luigi.Task):

    def requires(self):
        pass

    def run(self):
        engine = create_engine(
            'postgresql://postgres:password123@localhost:5432/etl_db')
        extracted_data = pd.read_sql('SELECT * FROM amazon_sales_data', engine)
        extracted_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(EXTRACTED_DATA_DIR+"sales_data.csv")


class TransformSalesData(luigi.Task):

    def requires(self):
        return ExtractSalesData()

    def run(self):
        extracted_data = pd.read_csv(self.input().path)
        extracted_data = extracted_data[[
            'name', 'main_category', 'sub_category', 'image', 'link',
            'ratings', 'no_of_ratings', 'discount_price', 'actual_price',
        ]]

        # Clean up column 'ratings', 'no_of_rating'
        cleaned_data = set_rating_column(extracted_data, 'ratings')
        cleaned_data = set_rating_column(cleaned_data, 'no_of_ratings')

        # Clean up column 'discount_price', 'actual_price'
        cleaned_data = set_money_column(cleaned_data, 'discount_price')
        cleaned_data = set_money_column(cleaned_data, 'actual_price')

        # Categorize 'home, kitchen, pets' as 'home & kitchen'
        cleaned_data = replace_column(extracted_data, 'main_category', {
            'home, kitchen, pets': 'home & kitchen',
        })

        # Keep 1 record for each duplicated data
        cleaned_data = cleaned_data.drop_duplicates(keep='first')

        # Remove records with 'actual_price' > 0 as it's not relevant sales
        cleaned_data = cleaned_data[cleaned_data['actual_price'] > 0]

        transformed_data = cleaned_data.sort_values(
            by=['name', 'sub_category', 'main_category'],
        )
        transformed_data = transformed_data.reset_index(drop=True)
        transformed_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(TRANSFORMED_DATA_DIR+"sales_data.csv")


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


if __name__ == "__main__":
    luigi.build([
        ExtractSalesData(), TransformSalesData(), LoadSalesData(),
    ], local_scheduler=True)
