import time
from src.main.model.model_util import predict_price
from src.main.utility.logging_config import logger
global predictions
# dummy 30-point history
hist = [100 + i*0.1 for i in range(30)]
n = 20
start = time.time()
for _ in range(n):
    predictions = predict_price(hist, horizon=30)
elapsed = time.time() - start
logger.info(f"Predictions: {predictions}")

