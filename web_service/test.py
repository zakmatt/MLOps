import requests


ride = {
    "PULocationID": 10,
    "DOLocationID": 50,
    "distance": 50
}

url = "http://localhost:9696/predict"
prediction = requests.post(url, json=ride)

print(prediction.json())