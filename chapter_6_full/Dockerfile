FROM public.ecr.aws/lambda/python:3.9


# Verify AWS CLI installation
#ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie /usr/local/bin/aws-lambda-rie
#RUN chmod +x /usr/local/bin/aws-lambda-rie

RUN pip install --upgrade pip
RUN pip install pipenv 

COPY [ "Pipfile", "Pipfile.lock", "./" ]

# Install dependencies
RUN pipenv install --system --deploy

COPY ["lambda_function.py", "model.py", "./"]

# Set the entrypoint to the AWS Lambda RIE
#ENTRYPOINT ["/usr/local/bin/aws-lambda-rie", "python3", "-m", "awslambdaric"]

CMD [ "lambda_function.lambda_handler" ]
