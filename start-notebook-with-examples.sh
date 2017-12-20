#!/bin/bash
set -e

if [ ! -d ~/work/examples ]; then
   mkdir ~/work/examples
   cp /forge/examples/*.ipynb ~/work/examples/
fi

. /usr/local/bin/start.sh jupyter notebook $*
