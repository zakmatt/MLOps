#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import pickle
import sys

from sklearn.pipeline import Pipeline

CATEGORICAL = ['PULocationID', 'DOLocationID']
TARGET = 'duration'

def read_data(file_name, year, month):
    df = pd.read_parquet(file_name)
    df[TARGET] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df[TARGET] = df[TARGET].dt.total_seconds() / 60
    df = df[(df[TARGET] >= 1) & (df[TARGET] <= 60)].copy()
    df["ride_id"] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    return df

def prepare_data(df):
    df[CATEGORICAL] = df[CATEGORICAL].fillna(-1).astype('int').astype('str')
    X = df[CATEGORICAL].to_dict(orient='records')
    target = df[TARGET].values

    return X, target


def load_model(model_path):
    with open(model_path, "rb") as f:
        dv, lr = pickle.load(f)

    model = Pipeline([
        ('vectorizer', dv),
        ('regression', lr)
    ])

    return model

def apply_model(file_name, output_file, model_path, year, month):
    model = load_model(model_path)
    data = read_data(file_name, year, month)
    X, target = prepare_data(data)
    predictions = model.predict(X)

    print(predictions.mean())
    result = pd.DataFrame()
    result["ride_id"] = data["ride_id"]
    result[TARGET] = predictions

    result.to_parquet(
        output_file,
        engine='pyarrow'
    )

def run():
    taxi_type = sys.argv[1] # "yellow"
    year = int(sys.argv[2]) # 2023
    month = int(sys.argv[3]) # 3
    input_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
    output_file = f"processed_{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
    model_path = "model.bin"

    apply_model(input_file, output_file, model_path, year, month)


if __name__ == "__main__":
    run()



