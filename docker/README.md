# 🪐 Docker Image and Container for Big Data Harvest Server

## 🏃 Steps to build the docker image of the Big Data Harvest Server

#### 1. Update (if required) the following version in the `Dockerfile` with a compatible Big Data Harvest Server version.

| Variable                        | Description |
| ------------------------------- | ------------|
| big_data_harvest_server_version | The version of the Big Data Harvest Server release to be included in the docker image|

```    
# Set the following argument with a compatible Big Data Harvest Server version
ARG big_data_harvest_server_version=1.0.0-SNAPSHOT
```

#### 2. Open a terminal and change the current working directory to `big-data-harvest-server/docker`.

#### 3. Build the docker image as follows.

```
docker image build --tag nasapds/big-data-harvest-server .
```

#### 4. As an optional step, push the docker image to a container image library.

For example, follow the below steps to push the newly built image to the Docker Hub.

* Execute the following command to log into the Docker Hub with a username and password (use the username and password of https://hub.docker.com/u/nasapds).
```
docker login
```
* Push the docker image to the Docker Hub.
```
docker image push nasapds/big-data-harvest-server
```
* Visit the Docker Hub (https://hub.docker.com/u/nasapds) and make sure that the `nasapds/big-data-harvest-server` image is available, so that it can be reused by other users without building it.


## 🏃 Steps to run a docker container of the Big Data Harvest Server

#### 1. Update the Big Data Harvest Server configuration file.

* Get a copy of the `harvest-server.cfg` file from https://github.com/NASA-PDS/big-data-harvest-server/blob/main/src/main/resources/conf/harvest-server.cfg and
keep it in a local file location such as `/tmp/cfg/harvest-server.cfg`.
* Update the properties such as `rmq.host`, `rmq.user`, `rmq.password` and `es.url` to match with your deployment environment.

#### 2. Update the following environment variables in the `run.sh`.

| Variable                   | Description |
| -------------------------- | ----------- |
| HARVEST_SERVER_CONFIG_FILE | # Absolute path for the Big Data Harvest Server configuration file in the host machine (E.g.: `/tmp/cfg/harvest-server.cfg`) |
| HARVEST_DATA_DIR           | Absolute path for the Harvest data directory in the host machine (E.g.: `/tmp/data/urn-nasa-pds-insight_rad`) |

```    
# Update the following environment variables before executing this script

# Absolute path for the Big Data Harvest Server configuration file in the host machine (E.g.: /tmp/cfg/crawler-server.cfg)
CRAWLER_SERVER_CONFIG_FILE=/tmp/cfg/crawler-server.cfg

# Absolute path for the Harvest data directory in the host machine (E.g.: /tmp/data/urn-nasa-pds-insight_rad)
HARVEST_DATA_DIR=/tmp/data
```

#### 3. Open a terminal and change the current working directory to `big-data-harvest-server/docker`.

#### 4. If executing for the first time, change the execution permissions of `run.sh` file as follows.

```
chmod u+x run.sh
```

#### 5. Execute the `run.sh` as follows.

```
./run.sh
```

Above steps will run a docker container of the Big Data Harvest Server.
