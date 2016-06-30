#!/usr/bin/env bash
#

echo "+----------------------------+"
echo "|Temporary script            |"
echo "|Need a more delicated script|"
echo "+----------------------------+"
###
###
ORG_DIR = pwd
PACK_DIR = "bottledwater-0.1"

make clean && make
cd
mkdir ${PACK_DIR}
mkdir ${PACK_DIR}/DEBIAN
mkdir ${PACK_DIR}/${ORG_DIR}
cp ${ORG_DIR}/control ${PACK_DIR}/DEBIAN/
cp -avr ${ORG_DIR}/client ${PACK_DIR}/${ORG_DIR}
cp -avr ${ORG_DIR}/ext ${PACK_DIR}/${ORG_DIR}
cp -avr ${ORG_DIR}/kafka ${PACK_DIR}/${ORG_DIR}
dpkg --build ${PACK_DIR}
rm -rf ${PACK_DIR}

###
###
echo "+----------------------------+"
echo "|Temporary script            |"
echo "|Need a more delicated script|"
echo "|            DONE            |"
echo "+----------------------------+"
