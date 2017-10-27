
# AthenaX: SQL-based streaming analytics platform at scale

Welcome to the documentation portal of AthenaX!

AthenaX is a streaming analytics platform that enables users to run production-quality, large-scale streaming analytics using Structured Query Language (SQL). AthenaX was released and open sourced by [Uber Technologies][ubeross]. It is capable of scaling across hundreds of machines and processing hundreds of billions of real-time events daily.

If you can't find what you are looking for, we'd love to hear from you either on [Github](https://github.com/uber/AthenaX/issues), or on our [mailing list](https://groups.google.com/forum/#!forum/athenax-users).

## Features

  * Streaming SQL
    * Filtering, projecting and combining streams
    * Aggregation on [group windows](https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/table/sql.html#group-windows) over both processing and event time
    * User-defined functions (UDF), User-defined aggregation function (UDAF), and User-defined table functions (UDTF) (coming soon)
  * Efficient executions through optimizations and code generations
  * Mechanisms to automatically fail over across multiple data centers
  * Auto scaling for AthenaX jobs

## Technical Specs

 * Built on top of [Apache Calcite](http://calcite.apache.org/) and [Apache Flink](http://flink.apache.org/)
 * [LevelDB](https://github.com/google/leveldb) as persistent storage

We published a [blog post](https://eng.uber.com/athenax) to describe the design and architecture of AthenaX.

## Related links
- [Introducing AthenaX, Uber Engineering’s Open Source Streaming Analytics Platform](https://eng.uber.com/athenax/)

[ubeross]: http://uber.github.io
