FROM python:3.9

ENV DOCKER_CONTAINER="True"

WORKDIR /fhir-load

COPY requirements.txt .

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir --upgrade -r requirements.txt

COPY . .

ENV PYTHONPATH "${PYTHONPATH}:/fhir-load"

LABEL maintainer="Abu Hasan" \
      version="1.0"

ENTRYPOINT ["python", "fhir-load.py"]