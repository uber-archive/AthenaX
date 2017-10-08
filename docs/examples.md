# Examples

This section briefly describes a few examples on building streaming analytic applications using SQL. Please read Flink's [Table and SQL API](https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/table/sql.html) for more details.

## Transforming a stream

The follow query chooses two fields in the table:

```sql
SELECT
  a, b
FROM
  orders
```

## Aggregation over group windows

Aggregation over a group window is one of the most common usage pattern in streaming analytics. The following query computes the total number of orders over the last 10 minutes:

```sql
SELECT
  COUNT(*)
FROM
  orders
GROUP BY
  HOP(rowtime, INTERVAL '10' MINUTE, INTERVAL '1' MINUTE)
```

## Using user-defined functions

AthenaX supports using user-defined functions (UDFs) in the query. The UDF is available in the query once it is registered through the [CREATE FUNCTION](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL) statement. For example, the following query uses the `GetRegion` UDF to compute the region id from the longitude and the latitude fields in the order table:

```sql
CREATE FUNCTION
  GetRegion
AS
  'com.uber.athenax.geo.GetRegion'
USING JAR
  'http://.../geo.jar';

SELECT
  GetRegion(lat, lng)
FROM
  orders
```
