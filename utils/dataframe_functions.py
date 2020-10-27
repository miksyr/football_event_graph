
def replace_nan_with_none_in_dataframe(dataframe):
    dataframe = dataframe.where(dataframe.notnull(), None)
    return dataframe.dropna(axis=0, how='all')
