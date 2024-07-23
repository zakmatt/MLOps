import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__)))

import model

PREDICTIONS_STREAM_NAME = os.getenv("PREDICTIONS_STREAM_NAME", "ride_predictions")
RUN_ID = os.getenv("RUN_ID")
TEST_RUN = os.getenv("TEST_RUN", "False") == "True"


model_service = model.init(
    prediction_stream_name=PREDICTIONS_STREAM_NAME,
    run_id=RUN_ID,
    test_run=TEST_RUN,
)


def lambda_handler(event, context):
    # pylint: disable=[unused-argument, missing-function-docstring]
    return model_service.lambda_handler(event)
