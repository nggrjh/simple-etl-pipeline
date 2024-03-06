
import pandas as pd

from datetime import datetime


def set_float_column(df, col):
    df[col] = df[col].fillna("-")
    df[col] = df[col].str.replace(",", "").replace("", "0")
    df.loc[df[col].str.contains(r'[^0-9.]', regex=True), col] = "0"
    df[col] = df[col].astype(float)
    return df


def set_money_column(df, col, currency):
    df[col] = df[col].fillna("").str.replace(currency, "")
    return set_float_column(df, col)


def replace_column(df, col, map):
    df[col] = df[col].replace(map)
    return df


def concate_date(value):
    if pd.isna(value):
        return ""

    date_strings = [date.strip() for date in value.split(",")]
    date_objects = [datetime.fromisoformat(date_str)
                    for date_str in date_strings]
    sorted_dates = sorted(date_objects, reverse=True)
    sorted_date_strings = [date.strftime("%Y-%m-%d") for date in sorted_dates]
    return ", ".join(sorted_date_strings)


def concate_string(value):
    if pd.isna(value):
        return ""

    strings = [date.strip() for date in value.split(",")]
    sorted_strings = sorted(strings, reverse=True)
    return ", ".join(sorted_strings)
