#!/bin/sh
mkdir -p /tmp/scratch/blueapi
blueapi -c /blueapi/config.yaml setup-scratch
blueapi -c /blueapi/config.yaml serve
