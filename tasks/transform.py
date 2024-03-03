import datetime
import json
import pandas as pd

from tasks.extract import *
from utils.data_type import *


TRANSFORMED_DATA_DIR = "data/transformed/"


class TransformSalesData(luigi.Task):

    def requires(self):
        return ExtractSalesData()

    def run(self):
        extracted_data = pd.read_csv(self.input().path)

        # Select only necessary columns
        extracted_data = extracted_data[[
            "name", "main_category", "sub_category", "image", "link",
            "ratings", "no_of_ratings", "discount_price", "actual_price",
        ]]

        # Clean up column "ratings", "no_of_rating"
        cleaned_data = set_float_column(extracted_data, "ratings")
        cleaned_data = set_float_column(cleaned_data, "no_of_ratings")

        # Clean up column "discount_price", "actual_price"
        cleaned_data = set_money_column(cleaned_data, "discount_price", "₹")
        cleaned_data = set_money_column(cleaned_data, "actual_price", "₹")

        # Categorize "home, kitchen, pets" as "home & kitchen"
        cleaned_data = replace_column(extracted_data, "main_category", {
            "home, kitchen, pets": "home & kitchen",
        })

        # Keep 1 record for each duplicated data
        cleaned_data = cleaned_data.drop_duplicates(keep="first")

        # Remove records with "actual_price" > 0 as it"s not relevant sales
        cleaned_data = cleaned_data[cleaned_data["actual_price"] > 0]

        transformed_data = cleaned_data.reset_index(drop=True)
        transformed_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(TRANSFORMED_DATA_DIR+"sales_data.csv")


class TransformMarketingData(luigi.Task):

    def requires(self):
        return ExtractMarketingData()

    def run(self):
        extracted_data = pd.read_csv(self.input().path)

        # Renamed columns for easy usage
        cleaned_data = extracted_data.rename(columns={
            "id": "uid",
            "prices.amountMax": "amount_max",
            "prices.amountMin": "amount_min",
            "prices.availability": "availability",
            "prices.condition": "condition",
            "prices.currency": "currency",
            "prices.dateSeen": "date_seen",
            "prices.isSale": "is_sale",
            "prices.merchant": "merchant",
            "prices.shipping": "shipping",
            "prices.sourceURLs": "source_urls",
            "dateAdded": "date_added",
            "dateUpdated": "date_updated",
            "imageURLs": "image_urls",
            "manufacturerNumber": "manufacturer_number",
            "primaryCategories": "primary_categories",
            "id": "uid",
        })

        # Define consistent enum for 'availability'
        cleaned_data = replace_column(cleaned_data, "availability", {
            "Yes": "In Stock",
            "yes":  "In Stock",
            "TRUE":  "In Stock",
            "32 available":  "In Stock",
            "7 available":  "In Stock",
            "undefined":  "Out Of Stock",
            "No":  "Out Of Stock",
            "sold":  "Out Of Stock",
            "FALSE":  "Out Of Stock",
            "Retired":  "Out Of Stock",
            "More on the Way": "Out Of Stock",
        })

        # Define consistent enum for 'condition'
        cleaned_data["condition"] = cleaned_data["condition"].\
            apply(self.replace_value)
        cleaned_data = replace_column(cleaned_data, "condition", {
            "pre-owned": "Used",
        })

        # Only found 1 CAD, assume that it should only records USD currency
        cleaned_data = cleaned_data[cleaned_data["currency"] == "USD"]

        cleaned_data = set_money_column(cleaned_data, "shipping", "USD ")
        cleaned_data = set_float_column(cleaned_data, "ean")

        cleaned_data["manufacturer"] = cleaned_data["manufacturer"].\
            fillna('')

        # Extract multiple values column to more readable format
        cleaned_data['date_seen'] = cleaned_data["date_seen"].\
            apply(convert_to_date_array)

        # Combine similar columns
        cleaned_data['source_urls'] += "," + cleaned_data['sourceURLs']
        cleaned_data['source_urls'] = cleaned_data["source_urls"].\
            apply(convert_to_string_array)
        cleaned_data = cleaned_data.drop(columns=["sourceURLs"])

        cleaned_data['image_urls'] = cleaned_data["image_urls"].\
            apply(convert_to_string_array)

        # Separate value and unit from 'weight'
        extracted_data[["weight_value", "weight_unit"]] = \
            extracted_data['weight'].str.split(n=1, expand=True)
        extracted_data = set_float_column(extracted_data, "weight_value")

        # Select only necessary columns
        cleaned_data = cleaned_data[[
            "uid", "name", "primary_categories", "categories", "condition",
            "availability", "brand", "merchant", "currency", "amount_max",
            "amount_min", "shipping", "is_sale", "weight",  "weight_value",
            "weight_unit", "manufacturer", "manufacturer_number", "source_urls",
            "image_urls", "asins", "ean", "keys", "upc", "date_seen",  "date_added", "date_updated",
        ]]

        transformed_data = cleaned_data.reset_index(drop=True)
        transformed_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(TRANSFORMED_DATA_DIR+"marketing_data.csv")

    def replace_value(value):
        if "new" in value.lower():
            return "New"
        elif "refurbished" in value.lower():
            return "Refurbished"
        else:
            return value
