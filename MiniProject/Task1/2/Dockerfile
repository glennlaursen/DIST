# syntax=docker/dockerfile:1
FROM python:3.7-alpine
WORKDIR /code
RUN apk add --no-cache gcc musl-dev linux-headers
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 5558
EXPOSE 5559
EXPOSE 5560
EXPOSE 5561
COPY . .
CMD python -u storage-node.py $name