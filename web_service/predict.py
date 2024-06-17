import pickle

from flask import Flask, request, jsonify

app = Flask("duration-prediction")

with open("./linear_model.pkl", "rb") as f:
    (dv, lr) = pickle.load(f)

def prepare_features(features):
    new_features = dict(
        PULocationID=int(features["PULocationID"]),
        DOLocationID=int(features["DOLocationID"]),
        distance=int(features["distance"])
    )

    return new_features

def predict(features):
    x = dv.transform(features)
    return lr.predict(x)

@app.route("/predict", methods=["POST"])
def predict_endpoint():
    ride = request.get_json()
    features = prepare_features(ride)
    prediction = predict(features)

    print(prediction)
    return jsonify(
        {
            "duration": float(prediction)
        }
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9696, debug=True)
