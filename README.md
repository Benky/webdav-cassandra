webdav-cassandra
================

This is proof-of-concept I made during one day hackhaton to see, if I can make webdav server which is backed by Cassandra (mainly for file metadata). Current implementation stores uploaded files also to Cassandra, but it can be quite easily modified to store uploaded files to some third party storage (S3, Cloud Files, ...).

Usage
-----

Please see application's help:
```
usage: WebdavServer
Cassandra-WebDav PoC
 -h,--help           print usage information
 -m,--create-model   creates model in cassandra
 -p,--port <arg>     port to bind to
```

First time you use this application (and don't have already created model), please use ``-m`` argument.

What's wrong
------------

- current model (mainly ``diretory`` CF) is wrong and needs to be redesigned
- there are no unit tests, JavaDoc is missing etc
- class design is bad
- it's not possible to configure cassandra connection
