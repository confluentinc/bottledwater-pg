#!/usr/bin/env bash
#

echo "+---------------+"
echo "|               |"
echo "+---------------+"

echo "Installing avro 1.8.1"
wget http://www-eu.apache.org/dist/avro/avro-1.8.1/c/avro-c-1.8.1.tar.gz
tar -xzvf avro-c-1.8.1.tar.gz
cd avro-c-1.8.1/
mkdir build
cd build
cmake ..  -DCMAKE_INSTALL_PREFIX=$PREFIX        -DCMAKE_BUILD_TYPE=RelWithDebInfo
make
make install
cd ../../
rm -rf avro-c-1.8.1/
rm avro-c-1.8.1.tar.gz
###
###
echo "+---------------+"
echo "|     DONE      |"
echo "+---------------+"
