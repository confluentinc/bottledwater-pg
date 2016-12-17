Summary: Bottledwater Extension for PostgreSQL 9.4
Name: bottledwater
Version: 0.5
Release: 1
Group: Applications/Extension
Packager: Dat Tran
AutoReqProv: no
BuildArch: noarch
License: No License

%description
This is an extension for PostgreSQL 9.4, it will stream all wal_log to bottledwater client

%files
%defattr(-,root,root)
%(echo $(pg_config --libdir))/bottledwater.so
%(echo $(pg_config --sharedir))/extension/bottledwater.control
%(echo $(pg_config --sharedir))/extension/bottledwater--0.1.sql
