echo "+----------------------------+"
echo "|Temporary script            |"
echo "|Need a more delicated script|"
echo "+----------------------------+"
###
###

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

cd ..
ORG_DIR=$(pwd)
PACK_DIR=avro-c-1.8
cd ..

mkdir -p ${PACK_DIR}/DEBIAN
mkdir -p ${PACK_DIR}/include/avro/
mkdir -p ${PACK_DIR}/lib/pkgconfig/
mkdir -p ${PACK_DIR}/bin/

cp -avr DEBIAN/AVRO_DEBIAN/* ${PACK_DIR}/DEBIAN/

cp -avr ${ORG_DIR}/build/src/avrocat ${PACK_DIR}/bin/
cp -avr ${ORG_DIR}/build/src/avroappend ${PACK_DIR}/bin/
cp -avr ${ORG_DIR}/build/src/avropipe ${PACK_DIR}/bin/
cp -avr ${ORG_DIR}/build/src/avromod ${PACK_DIR}/bin/

cp -avr ${ORG_DIR}/build/src/libavro* ${PACK_DIR}/lib/

cp -avr ${ORG_DIR}/build/src/avro-c.pc ${PACK_DIR}/lib/pkgconfig/

cp -avr ${ORG_DIR}/src/avro/* ${PACK_DIR}/include/avro/

cp -avr ${ORG_DIR}/src/avro.h ${PACK_DIR}/include/

dpkg --build ${PACK_DIR}
rm -rf ${PACK_DIR}

rm -rf avro-c-1.8.1
rm avro-c-1.8.1.tar.gz

###
###
echo "+----------------------------+"
echo "|Temporary script            |"
echo "|Need a more delicated script|"
echo "|            DONE            |"
echo "+----------------------------+"
