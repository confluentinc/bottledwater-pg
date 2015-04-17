Bugs / missing features
=======================

* Better support for more Postgres datatypes (see oid2avro.c).
* Record fields in autogenerated Avro schemas should default to null, so that adding
  and dropping columns is forward/backward compatible. (This may require patching libavro,
  as I think it doesn't support defaults at the moment.)
* JNI bindings, and/or other programming language support?