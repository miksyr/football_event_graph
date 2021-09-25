import pandas as pd


def replace_nan_with_none_in_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.where(dataframe.notnull(), None)
    return dataframe.dropna(axis=0, how="all")
