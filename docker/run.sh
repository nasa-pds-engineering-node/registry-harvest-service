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
# This script is used to start the Big Data Harvest Server docker container with a simple command.
#
# Usage: ./run.sh
#
# ----------------------------------------------------------------------------------------------

# Update the following environment variables before executing this script

# Elasticsearch URL (E.g.: http://192.168.0.1:9200)
ES_URL=http://192.168.0.1:9200

# Absolute path for the Big Data Harvest Server configuration file in the host machine (E.g.: /tmp/cfg/harvest-server.cfg)
HARVEST_SERVER_CONFIG_FILE=/tmp/cfg/harvest-server.cfg

# Absolute path for the Harvest data directory in the host machine (E.g.: /tmp/big-data-harvest-data/urn-nasa-pds-insight_rad).
# This directory will get created automatically, if the big-data-harvest-client is executed with the option to download test data.
HARVEST_DATA_DIR=/tmp/big-data-harvest-data


# Check if the Big Data Harvest Server configuration file exists
if [ ! -f "$HARVEST_SERVER_CONFIG_FILE" ]; then
    echo "Error: The Big Data Harvest Server configuration file $HARVEST_SERVER_CONFIG_FILE does not exist." \
            "Set an absolute file path for an existing Big Data Harvest Server configuration file in the $0 file" \
            "as the environment variable 'HARVEST_SERVER_CONFIG_FILE'." 1>&2
    exit 1
fi

# Check if the Harvest data directory exists
if [ ! -d "$HARVEST_DATA_DIR" ]; then
    echo "Error: The Harvest data directory $HARVEST_DATA_DIR does not exist." \
            "Set an absolute directory path for an existing Harvest data directory in the $0 file" \
            "as the environment variable 'HARVEST_DATA_DIR'." 1>&2
    exit 1
fi

# Execute docker container run to start the Big Data Harvest Server
docker container run --name big-data-harvest-server \
           --rm \
           --env ES_URL="${ES_URL}" \
           -p 8005:8005 \
           --volume "$HARVEST_SERVER_CONFIG_FILE":/cfg/harvest-server.cfg \
           --volume "$HARVEST_DATA_DIR":/data \
           nasapds/big-data-harvest-server
