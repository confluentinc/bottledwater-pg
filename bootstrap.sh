#!/usr/bin/env bash
#
# File:         bootstrap
# Description:  Initialize necessary packages for building bottlewater on centos

puts "+---------------+"
puts "| BOOTSTRAPPING |"
puts "+---------------+"

###
###
puts "Preparing centos repo environment..."
wget http://rpms.famillecollet.com/enterprise/6/remi/x86_64/remi-release-6.6-2.el6.remi.noarch.rpm /tmp
rpm -Uvh /tmp/remi-release*rpm

###
###
puts "Installing packages"
yum install -y zlib-devel.x86_64 snappy-devel.x86_64 gcc g++ jansson-devel.x86_64 libcurl-devel.x86_64 postgresql94-devel.x86_64
yum --enablerepo=remi install -y librdkafka-devel

###
###
puts "set up package config and build path"
export PKG_CONFIG_PATH=/usr/lib64/pkgconfig:/usr/lib/pkgconfig:/usr/lib/pkgconfig
export PATH=$PATH:/usr/pgsql-9.4/bin/


echo "Name: libsnappy \nDescription: Snappy is a compression library \nVersion: 1.1.2 \nURL: https://google.github.io/snappy/ \nLibs: -L/usr/lib -lsnappy \nCflags: -I/usr/include" > /usr/lib/pkgconfig/libsnappy.pc
