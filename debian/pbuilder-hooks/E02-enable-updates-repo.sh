#!/bin/sh -e

codename=$(lsb_release --codename --short)

# Add updates repo.  This is mostly to get a fixed version of the ssl-certs
# package, which is broken in precise on hosts with long hostnames, such as
# our chroot-within-Travis seems to have:
#
# https://bugs.launchpad.net/ubuntu/+source/ssl-cert/+bug/1004682

echo "deb http://archive.ubuntu.com/ubuntu/ ${codename}-updates main universe" >> /etc/apt/sources.list
