Summary: avro-c 1.8
Name: avro-c
Version: 1.8
Release: ub1
Group: Applications/Extension
Packager: Dat Tran
BuildRoot: %_topdir/%{name}
BuildArch: noarch
License: NoLicense

%description
avro-c 1.8

%prep

%build

%install

%clean

%files
%defattr(-,root,root)
%{_includedir}/avro.h
%{_avroincludedir}/data.h
%{_avroincludedir}/schema.h
%{_avroincludedir}/value.h
%{_avroincludedir}/legacy.h
%{_avroincludedir}/platform.h
%{_avroincludedir}/errors.h
%{_avroincludedir}/msinttypes.h
%{_avroincludedir}/allocation.h
%{_avroincludedir}/resolver.h
%{_avroincludedir}/refcount.h
%{_avroincludedir}/basics.h
%{_avroincludedir}/io.h
%{_avroincludedir}/consumer.h
%{_avroincludedir}/msstdint.h
%{_avroincludedir}/generic.h
%{_libdir}/libavro.a
%{_libdir}/libavro.so.23.0.0
%{_libdir}/libavro.so
%{_pkgconfigdir}/avro-c.pc
%{_bindir}/avrocat
%{_bindir}/avroappend
%{_bindir}/avropipe
%{_bindir}/avromod

%post

%postun
