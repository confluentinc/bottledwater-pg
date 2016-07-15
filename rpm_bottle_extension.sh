#!/usr/bin/env bash
#

echo "+----------------------------+"
echo "|Temporary script            |"
echo "|Need a more delicated script|"
echo "+----------------------------+"
###
###
ORG_DIR=$(pwd)
PG_SHAREDIR=$(pg_config --sharedir)
PG_LIBDIR=$(pg_config --libdir)
PACK_DIR=bottledwater-0.1

make clean && make
mkdir -p ${PACK_DIR}/RPMBUILD
mkdir -p ${PACK_DIR}/RPMBUILD/SPECS
mkdir -p ${PACK_DIR}/RPMBUILD/RPMS
mkdir -p ${PACK_DIR}/RPMBUILD/SRPMS
mkdir -p ${PACK_DIR}/RPMBUILD${PG_SHAREDIR}
mkdir -p ${PACK_DIR}/RPMBUILD${PG_LIBDIR}

cp -avr ${ORG_DIR}/ext/bottledwater.so ${PACK_DIR}/RPMBUILD${PG_SHAREDIR}
cp -avr ${ORG_DIR}/ext/bottledwater.control ${PACK_DIR}/RPMBUILD${PG_LIBDIR}
cp -avr ${ORG_DIR}/ext/bottledwater--0.1.sql ${PACK_DIR}/RPMBUILD${PG_LIBDIR}

cp -avr ${ORG_DIR}/RPM/.rpmmacros ~/

rpmbuild -bb ${ORG_DIR}/RPM/BOTTLE_RPM/bottle.spec

rm -rf ${PACK_DIR}

###
###
echo "+----------------------------+"
echo "|Temporary script            |"
echo "|Need a more delicated script|"
echo "|            DONE            |"
echo "+----------------------------+"
