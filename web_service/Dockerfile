FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim


WORKDIR /app
RUN pip install --upgrade pip
RUN pip install pipenv 
# Install additional tools for diagnostics
RUN apt-get update && apt-get install -y \
    bash \
    && rm -rf /var/lib/apt/lists/*

COPY ["Pipfile", "Pipfile.lock", "./"]
RUN pipenv install --system --deploy
COPY score.py /app/

CMD ["bash"]