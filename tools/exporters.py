""" ways to export files and data """

import io

# export dataframe as csv to string in memory
def exportCSV(df, index=False):
  with io.StringIO() as buffer:
    df.to_csv(buffer, index=index)
    return buffer.getvalue()