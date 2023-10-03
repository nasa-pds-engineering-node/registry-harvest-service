# 🪐 Docker Image and Container for Registry Harvest Service

This directory contains Docker-related information for making and running a containerized version of the Registry Harvest Service.


## 🧱 Building the Image

There's one build argument (normally specified with `--build-arg`) that you can specify at image construction time: `registry_harvest_service_url`. This should be set to the URL of the _binary distribution_ (in a gzip'd tar file) of the Registry Harvest Service that will be installed into the image.

Here are some example uses of the build argument:

1. `--build-arg "registry_harvest_service_url=https://github.com/NASA-PDS/registry-harvest-service/releases/download/v1.2.0/registry-harvest-service-1.2.0-bin.tar.gz"` — uses version 1.2.0 from GitHub
2. `--build-arg "registry_harvest_service_url=file:/Users/tloubrieu/PDS/registry-harvest-service/target/registry-harvest-service-1.3.0-SNAPSHOT-bin.tar.gz"` — uses a locally built version of 1.3.0-SNAPSHOT

As of this writing, if no `--build-arg` for `registry_harvest_service_url` is given then №1 above is the default.

Then, head over to the `registry-harvest-service/docker` directory and run:

    docker image build BUILD-ARG --tag nasapds/registry-harvest-service .

to build the image. Replace `BUILD_ARG` as needed.

As an optional step, push the newly created image to a container hub. For example, to push it to Docker Hub using the credentials for https://hub.docker.com/u/nasapds:
```console
$ docker login
$ docker image push nasapds/registry-harvest-service
```

Visit the Docker Hub (https://hub.docker.com/u/nasapds) and make sure that the `nasapds/registry-harvest-service` image is available, so that it can be reused by other users without building it.


## 🏃 Running a Docker Container of the Registry Harvest Service

To run the containerized Registry Harvest Service, do the following:

1. Update the Registry Harvest Service configuration file.

- Get a copy of the `harvest-server.cfg` file from https://github.com/NASA-PDS/registry-harvest-service/blob/main/src/main/resources/conf/harvest-server.cfg and keep it in a local file location such as `/tmp/cfg/harvest-server.cfg`. You may want to run it through `json_pp` to make it more readable.
- Update the properties such as `rmq.host`, `rmq.user`, `rmq.password` and `es.url` to match with your deployment environment.

2. Update the following environment variables in the `run.sh`.

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

**👉 Note:** Make sure to have the same `HARVEST_DATA_DIR` value set in the environment variables of the Registry Harvest Service, Registry Crawler Service and Registry Harvest CLI. Also, this `HARVEST_DATA_DIR` location should be accessible from the docker containers of the Registry Harvest Service, Registry Crawler Service and Registry Harvest CLI.

3. Open a terminal and change the current working directory to `registry-harvest-service/docker`.

4. If executing for the first time, change the execution permissions of `run.sh` file as follows.
```console
$ chmod u+x run.sh
```

5. Execute the `run.sh` as follows.
``` console
$ ./run.sh
```

Above steps will run a Docker container of the Registry Harvest Service.
