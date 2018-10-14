import pandas as pd
import numpy as np

def convert_dtypes(mydf):
    dtypes = mydf.dtypes
    for i, v in dtypes.items():
        if v == 'bool':
            mydf[i] = mydf[i].astype("int64")
        if v == 'datetime64[ns]':
            mydf[i] = mydf[i].dt.strftime('%-m/%-d/%Y %H:%M:%S')
    return mydf