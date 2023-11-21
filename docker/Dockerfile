# Dockerfile for Registry Harvest Service
# =======================================
#
# Copyright © 2023, California Institute of Technology ("Caltech").
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
#
# Basis
# -----
#
# This is Java, so we'll start off with a nice slim version of OpenJDK, 11.

FROM openjdk:18-slim

# Set the following argument with a compatible Registry Harvest Service version
ARG registry_harvest_service_version=1.2.1

# Build Arguments
# ---------------
#
# The sole build argument is for the URL to the Registry Harvest Service
# binary distribution. By default, that'll be version 1.2.0 taken
# from the GitHub releases. However, you can override this, such as with a
# `file:` URL, to a locally built release if you're testing out changes.
ENV registry_harvest_service_url=https://github.com/NASA-PDS/registry-harvest-service/releases/download/v${registry_harvest_service_version}/registry-harvest-service-${registry_harvest_service_version}-bin.tar.gz


# Image Layers
# ------------
#
# What we actually put into the image. We install and run `curl` instead of
# using Docker's `ADD` because the latter hides errors. For example, if you
# `ADD` a URL that's a 404, it will bizarrely add the current directory!

RUN : &&\
    apt-get update --quiet --yes &&\
    apt-get install --quiet --yes curl &&\
    curl --verbose --location --fail "$registry_harvest_service_url" >/tmp/registry-harvest-service-bin.tar.gz &&\
    : Sanity check to make sure we got some bytes &&\
    [ -s /tmp/registry-harvest-service-bin.tar.gz ] || exit 1 &&\
    mkdir /opt/registry-harvest-service &&\
    tar --extract --gzip --file /tmp/registry-harvest-service-bin.tar.gz --directory /opt/registry-harvest-service --strip-components 1 &&\
    rm /tmp/registry-harvest-service-bin.tar.gz &&\
    apt-get autoclean --quiet --yes &&\
    rm --recursive --force /var/lib/apt/lists/*


# Environment
# -----------
#
# Make sure we can execute commands out of the new bin dir.

ENV PATH="$PATH:/opt/registry-harvest-service/bin"


# Entry point
# -----------
#
# Where the magic happens.

COPY ./entrypoint.sh /usr/local/bin/entrypoint.sh
ENTRYPOINT ["bash", "/usr/local/bin/entrypoint.sh"]


# Metadata
# --------
#
# Does anyone actually use this?

LABEL "org.label-schema.name"="PDS Registry Harvest Service"
LABEL "org.label-schema.description"="Supports the capturing and indexing of product metadata into the PDS Registry system"
