import pandas as pd

from tasks.extract import *
from utils.data_type import *


TRANSFORMED_DATA_DIR = "data/transformed/"


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
            by=['main_category', 'sub_category'],
            ignore_index=True,
        )
        transformed_data = transformed_data.reset_index(drop=True)
        transformed_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(TRANSFORMED_DATA_DIR+"sales_data.csv")
