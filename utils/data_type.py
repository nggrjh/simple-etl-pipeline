
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
