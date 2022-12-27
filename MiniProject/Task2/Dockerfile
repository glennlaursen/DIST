# syntax=docker/dockerfile:1
FROM python:3.8-bullseye
WORKDIR /code
#RUN apt-get update -y && apt-get install gcc-4.9 -y && apt-get install libstdc++6 -y
#RUN apt-get dist-upgrade -y
#RUN export LD_LIBRARY_PATH=/usr/local/lib64:$LD_LIBRARY_PATH
#RUN apk add --no-cache gcc musl-dev linux-headers
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 5558
EXPOSE 5559
EXPOSE 5560
EXPOSE 5561
COPY . .
CMD python -u storage-node.py $name