# OZONE Commands

Ozone Doku: <https://ozone.apache.org/docs/1.2.0/start/startfromdockerhub.html>

## Start Ozone in standalone and local mode

`docker run -p 9878:9878 -p 9876:9876 apache/ozone:1.2.0`

## Install the AWS Client

Instructions for installing the aws client can be found here: <https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html>

## Commands for ozone

The Web UI is accessible under <http://localhost:9876/>

## Config AWS Console in the OZONE container

Login in OZONE container via `docker exec -it ... /bin/sh`

On the first login execute `aws configure` and set fake credentials.

## Start Ozone in cluster mode

`docker-compose up -d`

Login in OZONE container via `docker exec -it ozone_s3g_1 /bin/sh`

On the first login execute `aws configure` and set fake credentials.

Create a volume: `ozone sh volume create /vol1`

## Commands on the AWS console

Create Bucket: `aws s3api --endpoint http://localhost:9878/ create-bucket --bucket=bucket1`

Upload a file to the bucket: `aws s3 --endpoint http://localhost:9878 cp --storage-class REDUCED_REDUNDANCY  ../../data/iris.data  s3://bucket1/iris.data`

List the data: `aws s3 --endpoint http://localhost:9878 ls s3://bucket1/`

## Create random data

`ozone freon randomkeys --numOfVolumes=10 --numOfBuckets 10 --numOfKeys 10  --replicationType=RATIS --factor=ONE`
