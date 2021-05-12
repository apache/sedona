#!/bin/bash

set -efux -o pipefail

sudo DEBIAN_FRONTEND=noninteractive apt-get -y install libcurl4-openssl-dev

