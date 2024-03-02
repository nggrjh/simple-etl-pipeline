import pandas as pd
from sqlalchemy import create_engine


def set_float_column(df, col):
    df.loc[:, col] = df[col].str.replace(',', '').replace('', '0')
    df.loc[df[col].str.contains(r'[^0-9.]', regex=True), col] = '0'
    df.loc[:, col] = df[col].astype(float)
    return df


def set_rating_column(df, col):
    df.loc[:, col] = df[col].fillna('')
    return set_float_column(df, col)


def set_money_column(df, col):
    df.loc[:, col] = df[col].fillna('').str.replace('₹', '')
    return set_float_column(df, col)


def replace_column(df, col, map):
    df.loc[:, col] = df[col].replace(map)
    return df


conn = create_engine("postgresql://postgres:password123@localhost:5432/etl_db")
df = pd.read_sql("SELECT * FROM amazon_sales_data;", conn)

raw_data = df[["name", "main_category", "sub_category", "image", "link", "ratings", "no_of_ratings", "discount_price", "actual_price"]]

cleaned_data = set_rating_column(set_rating_column(raw_data, 'ratings'), 'no_of_ratings')
cleaned_data = set_money_column(set_money_column(cleaned_data, 'discount_price'), 'actual_price')
cleaned_data = replace_column(raw_data, 'main_category', {
    'home, kitchen, pets': 'home & kitchen',
})
cleaned_data = cleaned_data.drop_duplicates(keep='first')

sorted_by_category = cleaned_data.sort_values(by=["sub_category", "main_category"])
print(sorted_by_category.info())

grouped_by_category = sorted_by_category.groupby(["main_category", "sub_category"])
