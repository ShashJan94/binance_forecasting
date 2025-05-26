# src/main/model/prediction.py

import pandas as pd
# Import std flush and std out sys out for faster logging
import sys
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, DoubleType
from src.main.model.model_util import predict_price  # or whatever your import path is


@pandas_udf(ArrayType(DoubleType()))
def forecast_udf(past_series: pd.Series) -> pd.Series:
    sys.stdout.write(f"Incoming Data: {pd.Series(past_series)}\n")
    sys.stdout.flush()
    # past_series: Each row is a list/array of floats (your past_30)
    return past_series.apply(lambda x: predict_price(x, horizon=30))
