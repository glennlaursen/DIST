# syntax=docker/dockerfile:1
FROM python:3.8-bullseye
WORKDIR /code
#RUN apk add --no-cache gcc musl-dev linux-headers
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 5557
EXPOSE 5558
EXPOSE 5559
EXPOSE 5560
EXPOSE 5561
EXPOSE 5562
#EXPOSE 6666
COPY . .
CMD python -u storage-node.py $name