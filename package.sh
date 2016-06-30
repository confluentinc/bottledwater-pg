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
mkdir bottledwater-0.1
mkdir bottledwater-0.1/DEBIAN
mkdir bottledwater-0.1/root
cp bottledwater/control bottledwater-0.1/DEBIAN/
cp -avr bottledwater bottledwater-0.1/root/
dpkg --build bottlewater-0.1
rm -rf bottlewater-0.1

###
###
echo "+---------------+"
echo "|     DONE      |"
echo "+---------------+"
