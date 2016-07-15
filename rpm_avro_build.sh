echo "+----------------------------+"
echo "|Temporary script            |"
echo "|Need a more delicated script|"
echo "+----------------------------+"
###
###

BOTTLE_DIR=$(pwd)

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

mkdir -p ${PACK_DIR}/RPMBUILD
mkdir -p ${PACK_DIR}/RPMBUILD/SPECS
mkdir -p ${PACK_DIR}/RPMBUILD/RPMS
mkdir -p ${PACK_DIR}/RPMBUILD/SRPMS

cp -avr ${ORG_DIR}/build/src/avrocat %{_bindir}
cp -avr ${ORG_DIR}/build/src/avroappend %{_bindir}
cp -avr ${ORG_DIR}/build/src/avropipe %{_bindir}
cp -avr ${ORG_DIR}/build/src/avromod %{_bindir}

cp -avr ${ORG_DIR}/build/src/libavro* %{_libdir}

cp -avr ${ORG_DIR}/build/src/avro-c.pc %{_pkgconfigdir}

cp -avr ${ORG_DIR}/src/avro/* %{_avroincludedir}

cp -avr ${ORG_DIR}/src/avro.h %{_includedir}

cp -avr ${BOTTLE_DIR}/RPM/.rpmmacros ~/
rpmbuild -bb ${BOTTLE_DIR}/RPM/AVRO_RPM/avro.spec

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
