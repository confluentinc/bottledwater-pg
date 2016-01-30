#!/bin/sh

# Add stub/bottledwater PPA for its libavro package.
# N.B. We always use the trusty PPA even when building for precise, since the
# PPA doesn't have a precise build.  *shrug* Seems to work!

apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 6609E57DA8BDDEE3E10C90A1387ACAE2E4FD7A7A
echo deb http://ppa.launchpad.net/stub/bottledwater/ubuntu trusty main > /etc/apt/sources.list.d/ppa-bottledwater.list
