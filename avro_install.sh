#!/usr/bin/env bash
#

echo "+---------------+"
echo "|               |"
echo "+---------------+"

echo "Install dependencies"
sudo apt-get install libjansson-dev cmake libsnappy1 pkg-config

echo "Name: libsnappy" > /usr/share/pkgconfig/libsnappy.pc
echo "Description: Snappy is a compression library" >> /usr/share/pkgconfig/libsnappy.pc
echo "Version: 1.1.2" >> /usr/share/pkgconfig/libsnappy.pc
echo "URL: https://google.github.io/snappy/" >> /usr/share/pkgconfig/libsnappy.pc
echo "Libs: -L/usr/local/lib -lsnappy" >> /usr/share/pkgconfig/libsnappy.pc
echo "Cflags: -I/usr/local/include" >> /usr/share/pkgconfig/libsnappy.pc
echo "Installing avro 1.8.1"
wget http://www-eu.apache.org/dist/avro/avro-1.8.1/c/avro-c-1.8.1.tar.gz
tar -xzvf avro-c-1.8.1.tar.gz
cd avro-c-1.8.1/
mkdir build
cd build
cmake ..  -DCMAKE_INSTALL_PREFIX=$PREFIX        -DCMAKE_BUILD_TYPE=Release
make && make test
make install
cd ../../
rm -rf avro-c-1.8.1/
rm avro-c-1.8.1.tar.gz
###
###
echo "+---------------+"
echo "|     DONE      |"
echo "+---------------+"
