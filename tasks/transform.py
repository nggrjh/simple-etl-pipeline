import os
import re
import pandas as pd

from tasks.extract import *
from utils.data_type import *


TRANSFORMED_DATA_DIR = "data/transformed/"


class TransformSalesData(luigi.Task):

    def requires(self):
        return ExtractSalesData()

    def run(self):
        extracted_data = pd.read_csv(self.input().path)
        os.remove(self.input().path)

        # Select only necessary columns
        extracted_data = extracted_data[[
            "name", "main_category", "sub_category", "image", "link",
            "ratings", "no_of_ratings", "discount_price", "actual_price",
        ]]

        # Keep 1 record for each duplicated data
        cleaned_data = extracted_data.drop_duplicates(keep="first")

        # Clean up column "ratings", "no_of_rating"
        cleaned_data = set_float_column(cleaned_data, "ratings")
        cleaned_data = set_float_column(cleaned_data, "no_of_ratings")

        # Clean up column "discount_price", "actual_price"
        cleaned_data = set_money_column(cleaned_data, "discount_price", "₹")
        cleaned_data = set_money_column(cleaned_data, "actual_price", "₹")

        # Categorize "home, kitchen, pets" as "home & kitchen"
        cleaned_data = replace_column(cleaned_data, "main_category", {
            "home, kitchen, pets": "home & kitchen",
        })

        # Remove records with "actual_price" > 0 as it"s not relevant sales
        cleaned_data = cleaned_data[cleaned_data["actual_price"] > 0]
        cleaned_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(TRANSFORMED_DATA_DIR+"sales_data.csv")


class TransformMarketingData(luigi.Task):

    def requires(self):
        return ExtractMarketingData()

    def run(self):
        extracted_data = pd.read_csv(self.input().path)
        os.remove(self.input().path)

        # Keep 1 record for each duplicated data
        cleaned_data = extracted_data.drop_duplicates(keep="first")
        cleaned_data = cleaned_data.applymap(
            lambda x: x.strip() if isinstance(x, str) else x)

        # Renamed columns for easy usage
        cleaned_data = cleaned_data.rename(columns={
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

        cleaned_data["manufacturer"] = cleaned_data["manufacturer"].fillna("-")
        cleaned_data["primary_categories"] = cleaned_data["primary_categories"].str.strip()

        # Define consistent enum for "availability"
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

        # Define consistent enum for "condition"
        cleaned_data["condition"] = cleaned_data["condition"].\
            apply(self.normalize_condition)
        cleaned_data = replace_column(cleaned_data, "condition", {
            "pre-owned": "Used",
        })

        # Only found 1 CAD, assume that it should only records USD currency
        cleaned_data = cleaned_data[cleaned_data["currency"] == "USD"]
        cleaned_data = set_money_column(cleaned_data, "shipping", "USD ")

        cleaned_data["ean"] = cleaned_data["ean"].\
            apply(self.long_number)
        cleaned_data["upc"] = cleaned_data["upc"].\
            apply(self.long_number)

        # Extract multiple values column to more readable format
        cleaned_data["date_seen"] = cleaned_data["date_seen"].\
            apply(concate_date)

        # Combine similar columns
        cleaned_data["source_urls"] += "," + cleaned_data["sourceURLs"]
        cleaned_data["source_urls"] = cleaned_data["source_urls"].\
            apply(concate_string)
        cleaned_data = cleaned_data.drop(columns=["sourceURLs"])

        cleaned_data["image_urls"] = cleaned_data["image_urls"].\
            apply(concate_string)

        cleaned_data["weight"] = \
            cleaned_data["weight"].apply(self.trim_weight)

        cleaned_data[["size", "unit"]] = cleaned_data["weight"].str.\
            split("/", n=1, expand=True)
        cleaned_data.loc[cleaned_data["primary_categories"] ==
                         "Intel Celeron", "unit"] = "inches"

        # Define consistent enum for "unit"
        cleaned_data = replace_column(cleaned_data, "unit", {
            "oz": "ounces",
            "lb":  "pounds",
        })

        # Select only necessary columns
        cleaned_data = cleaned_data[[
            "uid", "name", "primary_categories", "categories", "condition",
            "availability", "brand", "merchant", "currency", "amount_max", "amount_min",
            "shipping", "is_sale", "size", "unit", "manufacturer",
            "manufacturer_number", "source_urls", "image_urls", "asins",
            "ean", "keys", "upc", "date_seen",  "date_added", "date_updated",
        ]]

        cleaned_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(TRANSFORMED_DATA_DIR+"marketing_data.csv")

    def normalize_condition(self, value):
        if "new" in value.lower():
            return "New"
        elif "refurbished" in value.lower():
            return "Refurbished"
        else:
            return value

    def long_number(self, value):
        if pd.isna(value):
            return "-"

        value = value.replace(",", "").replace('"', "").strip()

        if re.search(r'\D', value):
            return "-"

        return str(int(float(value)))

    def trim_weight(self, value):
        if pd.isna(value):
            return "/"

        value = value.strip()

        if not re.search(r'\d', value):
            return "-/-"

        weight_strings = re.sub(" +", " ", value).split(" ")

        if len(weight_strings) < 2:
            return value+"/"

        i = 0
        normalized_weights = []
        while i < len(weight_strings):
            value = weight_strings[i]
            unit = weight_strings[i+1]

            normalized_weights.append(value)

            i += 2

        return str(", ".join(normalized_weights))+"/"+unit


class TransformArticles(luigi.Task):

    def requires(self):
        return ExtractArticles()

    def run(self):
        extracted_data = pd.read_csv(self.input().path)
        os.remove(self.input().path)

        transformed_data = extracted_data.\
            sort_values(by="date", ascending=False, ignore_index=True)
        transformed_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(TRANSFORMED_DATA_DIR+"articles.csv")
