
import luigi
import pandas as pd
import requests

from bs4 import BeautifulSoup
from datetime import *
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


class ExtractArticles(luigi.Task):

    def requires(self):
        pass

    def run(self):

        data = []

        now = datetime.now()
        for x in range(0, 2):
            date = (now - timedelta(days=x)).strftime('%Y-%m-%d')

            i = 1
            while True:
                url = f"https://indeks.kompas.com/?date={date}&page={i}"

                resp = requests.get(url)
                soup = BeautifulSoup(resp.text, "html.parser")

                items = soup.find_all(class_="articleItem")

                if len(items) < 1:
                    break

                for item in items:
                    title = item.find(class_="articleTitle")
                    category = item.find(class_="articlePost-subtitle")
                    post_date = item.find(class_="articlePost-date")
                    link = item.find("a").get("href")
                    image = item.\
                        find(class_="articleItem-img").\
                        find("img").get("src")

                    data.append({
                        "title": title.text,
                        "category": category.text,
                        "date": post_date.text,
                        "link": link,
                        "thumbnail": image,
                    })

                i += 1

        extracted_data = pd.DataFrame(data)
        extracted_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(EXTRACTED_DATA_DIR+"articles.csv")
