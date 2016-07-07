#!/usr/bin/env bash
#

echo "+----------------------------+"
echo "|Temporary script            |"
echo "|Need a more delicated script|"
echo "+----------------------------+"
###
###
ORG_DIR=$(pwd)
PACK_DIR=bottledwater-0.1
EXECUTABLE=bottledwater

make clean && make && cd
mkdir -p ${PACK_DIR}/DEBIAN
mkdir -p ${PACK_DIR}${ORG_DIR}/kafka
cp ${ORG_DIR}/DEBIAN/control ${PACK_DIR}/DEBIAN/
cp -avr ${ORG_DIR}/kafka/${EXECUTABLE} ${PACK_DIR}${ORG_DIR}/kafka
dpkg --build ${PACK_DIR}
rm -rf ${PACK_DIR}

###
###
echo "+----------------------------+"
echo "|Temporary script            |"
echo "|Need a more delicated script|"
echo "|            DONE            |"
echo "+----------------------------+"
