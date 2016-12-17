Summary: avro-c
Name: avro-c
Version: 1.8
Release: 1
Group: Applications/Extension
Packager: Dat Tran
BuildArch: noarch
AutoReqProv: no
Requires: jansson, snappy, pkgconfig
License: NoLicense

%description
avro-c 1.8

%files
%defattr(-,root,root)
/include/avro.h
/include/avro/data.h
/include/avro/schema.h
/include/avro/value.h
/include/avro/legacy.h
/include/avro/platform.h
/include/avro/errors.h
/include/avro/msinttypes.h
/include/avro/allocation.h
/include/avro/resolver.h
/include/avro/refcount.h
/include/avro/basics.h
/include/avro/io.h
/include/avro/consumer.h
/include/avro/msstdint.h
/include/avro/generic.h
/lib/libavro.a
/lib/libavro.so.23.0.0
/lib/libavro.so
/lib/pkgconfig/avro-c.pc
/bin/avrocat
/bin/avroappend
/bin/avropipe
/bin/avromod


