# pylint: disable=duplicate-code

from deepdiff import DeepDiff
from pprint import pprint

import boto3
import json
import os
import requests
import time

KINESIS_ENDPOINT = os.getenv("KINESIS_ENDPOINT_URL", "http://localhost:4566")
KINESIS_CLIENT = boto3.client("kinesis", endpoint_url=KINESIS_ENDPOINT)

STREAM_NAME = os.getenv("PREDICTIONS_STREAM_NAME", "ride_predictions")
SHARD_ID = "shardId-000000000000"


def create_kinesis_stream(
    kinesis_client: boto3.session.Session.client, stream_name: str
):
    try:
        # Create a Kinesis stream in the localstack container
        print(f"Creating a kinesis stream {stream_name}")
        response = kinesis_client.create_stream(StreamName=stream_name, ShardCount=1)
    except kinesis_client.exceptions.ResourceInUseException:
        print(f"Stream {stream_name} already exists")
        return
    except Exception as e:
        print("An Error occurred!")
        print(f"Could not create a stream {stream_name}")
        print(e)
        return

    while True:
        response = kinesis_client.describe_stream(StreamName=stream_name)
        stream_status = response["StreamDescription"]["StreamStatus"]
        if stream_status == "ACTIVE":
            print(f"Stream {stream_name} is now active.")
            break
        print(f"Waiting for stream {stream_name} to become active...")
        time.sleep(5)


def put_event(event_path: str):
    url = "http://localhost:8080/2015-03-31/functions/function/invocations"

    with open(event_path, "r", encoding="utf-8") as f:
        event = json.load(f)

    true_response = requests.post(url=url, json=event)  # .json()
    print(true_response)
    true_response = true_response.json()

    print("True response:")
    print(json.dumps(true_response, indent=4))

    expected_response = {
        "predictions": [
            {
                "model": "ride_duration_prediction_model",
                "version": "TestId",
                "prediction": {
                    "ride_duration": 21.3,
                    "ride_id": 256,
                },
            }
        ]
    }

    diff = DeepDiff(true_response, expected_response, significant_digits=1)
    print(f"diff={diff}")

    assert "type_changes" not in diff
    assert "values_changed" not in diff


def test_kinesis(
    kinesis_client: boto3.session.Session.client, stream_name: str, shard_id: int
):

    shard_iterator_response = kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON",
    )

    shard_iterator_id = shard_iterator_response["ShardIterator"]

    records_response = kinesis_client.get_records(
        ShardIterator=shard_iterator_id,
        Limit=1,
    )

    records = records_response["Records"]
    pprint(records)

    assert len(records) == 1

    actual_record = json.loads(records[0]["Data"])
    pprint(actual_record)

    expected_record = {
        "model": "ride_duration_prediction_model",
        "version": "TestId",
        "prediction": {
            "ride_duration": 21.3,
            "ride_id": 256,
        },
    }

    diff = DeepDiff(actual_record, expected_record, significant_digits=1)
    print(f"diff={diff}")

    assert "values_changed" not in diff
    assert "type_changes" not in diff

    print("all good")


if __name__ == "__main__":
    create_kinesis_stream(KINESIS_CLIENT, STREAM_NAME)
    put_event("event.json")
    test_kinesis(KINESIS_CLIENT, STREAM_NAME, SHARD_ID)
