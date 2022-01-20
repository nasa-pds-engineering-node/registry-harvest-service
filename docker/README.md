# 🪐 Docker Image and Container for Big Data Harvest Server

## 🏃 Steps to build the docker image of the Big Data Harvest Server

#### 1. Update (if required) the following versions in the `Dockerfile` with a compatible versions.

| Variable                        | Description |
| ------------------------------- | ------------|
| big_data_harvest_server_version | The version of the Big Data Harvest Server release to be included in the docker image|
| reg_manager_version             | The version of the Registry Manager release to be included in the docker image|

```    
# Set the following arguments with compatible versions
ARG big_data_harvest_server_version=1.0.0-SNAPSHOT
ARG reg_manager_version=4.2.0
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
| ES_URL                     | Elasticsearch URL (E.g.: http://192.168.0.1:9200) |
| HARVEST_SERVER_CONFIG_FILE | Absolute path of the Big Data Harvest Server configuration file in the host machine (E.g.: `/tmp/cfg/harvest-server.cfg`) |
| HARVEST_DATA_DIR           | Absolute path of the Harvest data directory in the host machine (E.g.: `/tmp/big-data-harvest-data`). If the Big Data Harvest Client is executed with the option to download test data, then this directory will be cleaned-up and populated with test data |

```    
# Update the following environment variables before executing this script

# Elasticsearch URL (E.g.: http://192.168.0.1:9200)
ES_URL=http://192.168.0.1:9200

# Absolute path of the Big Data Harvest Server configuration file in the host machine (E.g.: /tmp/cfg/harvest-server.cfg)
HARVEST_SERVER_CONFIG_FILE=/tmp/cfg/harvest-server.cfg

# Absolute path of the Harvest data directory in the host machine (E.g.: `/tmp/big-data-harvest-data`). 
# If the Big Data Harvest Client is executed with the option to download test data, then this directory will be 
# cleaned-up and populated with test data. Make sure to have the same `HARVEST_DATA_DIR` value set in the 
# environment variables of the Big Data Harvest Server, Big Data Crawler Server and Big Data Harvest Client. 
# Also, this `HARVEST_DATA_DIR` location should be accessible from the docker containers of the Big Data Harvest Server, 
# Big Data Crawler Server and Big Data Harvest Client.
HARVEST_DATA_DIR=/tmp/big-data-harvest-data
```

Note:

Make sure to have the same `HARVEST_DATA_DIR` value set in the environment variables of the Big Data Harvest Server,
Big Data Crawler Server and Big Data Harvest Client. Also, this `HARVEST_DATA_DIR` location should be accessible from the
docker containers of the Big Data Harvest Server, Big Data Crawler Server and Big Data Harvest Client.


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
