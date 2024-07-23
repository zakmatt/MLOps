# Commands
## 1.
Build the docker container which only tests the lambda function
```bash


docker run -it --rm \
    -p 8080:8080 \
    -e PREDICTIONS_STREAM_NAME="ride_predictions" \
    -e RUN_ID="e1efc53e9bd149078b0c12aeaa6365df" \
    -e TEST_RUN="True" \
    -e MODEL_LOCATION="/app/model" \
    -e AWS_DEFAULT_REGION="eu-west-1" \
    -v ${PWD}/model:/app/model \
    image_name
```

## pylint
`pylint <file_name>.py`
`pylint --recursive=y <file_name>.py`
`black` formats the code an makes it look nicer  
`black --diff .` to see what to change
`black .` will change everything.
`isort` takes care of imports