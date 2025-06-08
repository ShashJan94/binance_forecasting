# src/main/model/prediction.py

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, DoubleType
from src.main.model.model_util import predict_price  # or whatever your import path is
import logging

# Configure logging for better control than print
# This will log from the Spark workers; you might need to configure Spark's log4j properties
# to see these logs effectively or use a more sophisticated logging setup.
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@pandas_udf(ArrayType(DoubleType()))
def forecast_udf(past_series: pd.Series) -> pd.Series:
    # More controlled logging: log the number of series to process
    # and perhaps the shape/type of the first item if needed for debugging.
    # logger.info(f"forecast_udf: Processing a batch of {len(past_series)} series.")
    # if not past_series.empty and past_series.iloc[0] is not None:
    #     logger.info(f"forecast_udf: First series example length: {len(past_series.iloc[0])}")

    def safe_predict(single_series_data):
        try:
            # Assuming predict_price expects a list/array and returns a list/array of doubles
            if single_series_data is None:  # Handle potential null inputs if they can occur
                return []  # Or an array of np.nan, matching horizon length if possible

            # Make sure the input to predict_price is in the expected format (e.g., list)
            # If predict_price handles pd.Series directly, this conversion might not be needed.
            # If it expects a numpy array, convert accordingly.
            data_for_prediction = list(single_series_data) if not isinstance(single_series_data,
                                                                             list) else single_series_data

            result = predict_price(data_for_prediction, horizon=30)

            # Ensure the result is a list of floats/doubles
            if not isinstance(result, list) or not all(isinstance(item, (float, int)) for item in result):
                # logger.error(f"predict_price returned unexpected type: {type(result)}. Data: {single_series_data[
                # :10]}") Return an empty list or NaNs of expected horizon length For example, if horizon is 30: [
                # float('nan')] * 30
                return [float('nan')] * 30  # Placeholder for error
            return [float(item) for item in result]
        except Exception as e:
            # Log the error and the data that caused it (be careful with logging sensitive data) logger.error(f"Error
            # in predict_price for series: {single_series_data[:10]}... - Error: {e}", exc_info=True) Return a value
            # that matches the UDF's return type (ArrayType(DoubleType())) e.g., an empty list or a list of NaNs for
            # the expected horizon.
            return [float('nan')] * 30  # Placeholder for error, assuming horizon 30

    return past_series.apply(safe_predict)
