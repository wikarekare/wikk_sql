#!/bin/sh
#Local checking. Creates pkg/
./gendoc.sh
/usr/local/bin/rake --trace gem

