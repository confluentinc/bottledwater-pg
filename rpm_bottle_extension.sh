#!/usr/bin/env bash
#

echo "+----------------------------+"
echo "|Temporary script            |"
echo "|Need a more delicated script|"
echo "+----------------------------+"
###
###

# yum install -y zlib-devel.x86_64 snappy-devel.x86_64 gcc g++ jansson-devel.x86_64 libcurl-devel.x86_64 postgresql94-devel.x86_64

ORG_DIR=$(pwd)
PG_SHAREDIR=$(pg_config --sharedir)
PG_LIBDIR=$(pg_config --libdir)
PACK_DIR=bottledwater-0.5

make clean && make
mkdir -p ${PACK_DIR}/RPMBUILD
mkdir -p ${PACK_DIR}/RPMBUILD${PG_SHAREDIR}/extension
mkdir -p ${PACK_DIR}/RPMBUILD${PG_LIBDIR}

cp -avr ${ORG_DIR}/ext/bottledwater.so ${PACK_DIR}/RPMBUILD${PG_LIBDIR}
cp -avr ${ORG_DIR}/ext/bottledwater.control ${PACK_DIR}/RPMBUILD${PG_SHAREDIR}/extension
cp -avr ${ORG_DIR}/ext/bottledwater--0.1.sql ${PACK_DIR}/RPMBUILD${PG_SHAREDIR}/extension

rpmbuild -bb --buildroot $(pwd)/${PACK_DIR}/RPMBUILD  ${ORG_DIR}/RPM/BOTTLE_RPM/bottle.spec

rm -rf ${PACK_DIR}
make clean

###
###
echo "+----------------------------+"
echo "|Temporary script            |"
echo "|Need a more delicated script|"
echo "|            DONE            |"
echo "+----------------------------+"
