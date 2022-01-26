#!/bin/sh

# Copyright 2022, California Institute of Technology ("Caltech").
# U.S. Government sponsorship acknowledged.
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# * Redistributions must reproduce the above copyright notice, this list of
# conditions and the following disclaimer in the documentation and/or other
# materials provided with the distribution.
# * Neither the name of Caltech nor its operating division, the Jet Propulsion
# Laboratory, nor the names of its contributors may be used to endorse or
# promote products derived from this software without specific prior written
# permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

# ----------------------------------------------------------------------------------------------
# This script is used to start the Registry Harvest Service docker container with a simple command.
#
# Usage: ./run.sh
#
# ----------------------------------------------------------------------------------------------

# Update the following environment variables before executing this script

# Elasticsearch URL (E.g.: http://192.168.0.1:9200)
ES_URL=http://192.168.0.1:9200

# Absolute path of the Registry Harvest Service configuration file in the host machine (E.g.: /tmp/cfg/harvest-server.cfg)
HARVEST_SERVER_CONFIG_FILE=/tmp/cfg/harvest-server.cfg

# Absolute path of the Harvest data directory in the host machine (E.g.: `/tmp/registry-harvest-data`).
# If the Registry Harvest CLI is executed with the option to download test data, then this directory will be
# cleaned-up and populated with test data. Make sure to have the same `HARVEST_DATA_DIR` value set in the
# environment variables of the Registry Harvest Service, Registry Crawler Server and Registry Harvest CLI.
# Also, this `HARVEST_DATA_DIR` location should be accessible from the docker containers of the Registry Harvest Service,
# Registry Crawler Server and Registry Harvest CLI.
HARVEST_DATA_DIR=/tmp/registry-harvest-data


# Check if the Registry Harvest Service configuration file exists
if [ ! -f "$HARVEST_SERVER_CONFIG_FILE" ]; then
    echo "Error: The Registry Harvest Service configuration file $HARVEST_SERVER_CONFIG_FILE does not exist." \
            "Set an absolute file path of an existing Registry Harvest Service configuration file in the $0 file" \
            "as the environment variable 'HARVEST_SERVER_CONFIG_FILE'." 1>&2
    exit 1
fi

# Check if the Harvest data directory exists
if [ ! -d "$HARVEST_DATA_DIR" ]; then
    echo "Error: The Harvest data directory $HARVEST_DATA_DIR does not exist." \
            "Set an absolute directory path of an existing Harvest data directory in the $0 file" \
            "as the environment variable 'HARVEST_DATA_DIR'." 1>&2
    exit 1
fi

# Execute docker container run to start the Registry Harvest Service
docker container run --name registry-harvest-service \
           --rm \
           --env ES_URL="${ES_URL}" \
           --publish 8005:8005 \
           --volume "$HARVEST_SERVER_CONFIG_FILE":/cfg/harvest-server.cfg \
           --volume "$HARVEST_DATA_DIR":/data \
           nasapds/registry-harvest-service
