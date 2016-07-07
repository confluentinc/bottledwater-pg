#!/usr/bin/env bash
#
# File:         bootstrap
# Description:  Initialize necessary packages for building bottlewater on ubuntu

echo "+---------------+"
echo "| BOOTSTRAPPING |"
echo "+---------------+"
###
###

echo "Installing packages"
sudo apt-get install postgresql-server-dev-9.5 libpq-dev libsnappy-dev libjansson-dev libcurl4-openssl-dev librdkafka-dev cmake pkg-config

###
###
echo "Name: libsnappy" > /usr/share/pkgconfig/libsnappy.pc
echo "Description: Snappy is a compression library" >> /usr/share/pkgconfig/libsnappy.pc
echo "Version: 1.1.2" >> /usr/share/pkgconfig/libsnappy.pc
echo "URL: https://google.github.io/snappy/" >> /usr/share/pkgconfig/libsnappy.pc
echo "Libs: -L/usr/lib -lsnappy" >> /usr/share/pkgconfig/libsnappy.pc
echo "Cflags: -I/usr/include" >> /usr/share/pkgconfig/libsnappy.pc

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
