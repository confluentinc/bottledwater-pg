Summary: Bottledwater Extension for PostgreSQL 9.4
Name: bottledwater
Version: 0.1
Release: ub1
Group: Applications/Extension
Packager: Dat Tran
BuildRoot: %_topdir/%{name}
BuildArch: noarch
License: NoLicense

%description
This is an extension for PostgreSQL 9.4, it will stream all wal_log to bottledwater client

%prep

%build

%install

%clean

%files
%defattr(-,root,root)
%{_pgsqllibdir}/bottledwater.so
%{_pgsqlsharedir}/bottledwater.control
%{_pgsqlsharedir}/bottledwater--0.1.sql

%post

%postun
