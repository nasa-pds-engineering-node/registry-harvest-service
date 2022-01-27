# 🪐 Docker Image and Container for Registry Harvest Service

## 🏃 Steps to build the docker image of the Registry Harvest Service

#### 1. Update (if required) the following Registry Harvest Service version in the `Dockerfile` with a compatible versions.

| Variable                        | Description |
| ------------------------------- | ------------|
| registry_harvest_service_version | The version of the Registry Harvest Service release to be included in the docker image|

```    
# Set the following argument with a compatible Registry Harvest Service version
ARG registry_harvest_service_version=1.0.0-SNAPSHOT
```

#### 2. Open a terminal and change the current working directory to `registry-harvest-service/docker`.

#### 3. Build the docker image as follows.

```
docker image build --tag nasapds/registry-harvest-service .
```

#### 4. As an optional step, push the docker image to a container image library.

For example, follow the below steps to push the newly built image to the Docker Hub.

* Execute the following command to log into the Docker Hub with a username and password (use the username and password of https://hub.docker.com/u/nasapds).
```
docker login
```
* Push the docker image to the Docker Hub.
```
docker image push nasapds/registry-harvest-service
```
* Visit the Docker Hub (https://hub.docker.com/u/nasapds) and make sure that the `nasapds/registry-harvest-service` image is available, so that it can be reused by other users without building it.


## 🏃 Steps to run a docker container of the Registry Harvest Service

#### 1. Update the Registry Harvest Service configuration file.

* Get a copy of the `harvest-server.cfg` file from https://github.com/NASA-PDS/registry-harvest-service/blob/main/src/main/resources/conf/harvest-server.cfg and
keep it in a local file location such as `/tmp/cfg/harvest-server.cfg`.
* Update the properties such as `rmq.host`, `rmq.user`, `rmq.password` and `es.url` to match with your deployment environment.

#### 2. Update the following environment variables in the `run.sh`.

| Variable                   | Description |
| -------------------------- | ----------- |
| ES_URL                     | Elasticsearch URL (E.g.: http://192.168.0.1:9200) |
| HARVEST_SERVER_CONFIG_FILE | Absolute path of the Registry Harvest Service configuration file in the host machine (E.g.: `/tmp/cfg/harvest-server.cfg`) |
| HARVEST_DATA_DIR           | Absolute path of the Harvest data directory in the host machine (E.g.: `/tmp/registry-harvest-data`). If the Registry Harvest CLI is executed with the option to download test data, then this directory will be cleaned-up and populated with test data |

```    
# Update the following environment variables before executing this script

# Elasticsearch URL (E.g.: http://192.168.0.1:9200)
ES_URL=http://192.168.0.1:9200

# Absolute path of the Registry Harvest Service configuration file in the host machine (E.g.: /tmp/cfg/harvest-server.cfg)
HARVEST_SERVER_CONFIG_FILE=/tmp/cfg/harvest-server.cfg

# Absolute path of the Harvest data directory in the host machine (E.g.: `/tmp/registry-harvest-data`). 
# If the Registry Harvest CLI is executed with the option to download test data, then this directory will be 
# cleaned-up and populated with test data. Make sure to have the same `HARVEST_DATA_DIR` value set in the 
# environment variables of the Registry Harvest Service, Registry Crawler Service and Registry Harvest CLI. 
# Also, this `HARVEST_DATA_DIR` location should be accessible from the docker containers of the Registry Harvest Service, 
# Registry Crawler Service and Registry Harvest CLI.
HARVEST_DATA_DIR=/tmp/registry-harvest-data
```

Note:

Make sure to have the same `HARVEST_DATA_DIR` value set in the environment variables of the Registry Harvest Service,
Registry Crawler Service and Registry Harvest CLI. Also, this `HARVEST_DATA_DIR` location should be accessible from the
docker containers of the Registry Harvest Service, Registry Crawler Service and Registry Harvest CLI.


#### 3. Open a terminal and change the current working directory to `registry-harvest-service/docker`.

#### 4. If executing for the first time, change the execution permissions of `run.sh` file as follows.

```
chmod u+x run.sh
```

#### 5. Execute the `run.sh` as follows.

```
./run.sh
```

Above steps will run a docker container of the Registry Harvest Service.
