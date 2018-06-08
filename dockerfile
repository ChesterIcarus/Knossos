FROM ubuntu:16.10
# FROM python:3.6.5-stretch

COPY ./*.py/ ./requirements.txt/ ./maz.geojson ./parcel.geojson ./valid_maz_for_bounds.json ./
WORKDIR /Knossos
RUN apt-get update -y
RUN apt-get install -y python3-pip python3.6-dev build-essential libspatialindex-dev
RUN pip install -r requirements.txt
CMD ["python", "u", "LinkinkApnToMaz.py"]
