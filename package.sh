#!/usr/bin/env bash
#

echo "+----------------------------+"
echo "|Temporary script            |"
echo "|Need a more delicated script|"
echo "+----------------------------+"
###
###

make clean && make
cd ..
rm -rf bottledwater-0.1
mkdir bottledwater-0.1
mkdir bottledwater-0.1/DEBIAN
mkdir bottledwater-0.1/root
mkdir bottledwater-0.1/root/bottledwater
cp bottledwater/control bottledwater-0.1/DEBIAN/
cp -avr bottledwater/client bottledwater-0.1/root/bottledwater/
cp -avr bottledwater/ext bottledwater-0.1/root/bottledwater/
cp -avr bottledwater/kafka bottledwater-0.1/root/bottledwater/
dpkg --build bottlewater-0.1
rm -rf bottlewater-0.1

###
###
echo "+----------------------------+"
echo "|Temporary script            |"
echo "|Need a more delicated script|"
echo "|            DONE            |"
echo "+----------------------------+"
