# Getting Started

## Building AthenaX and Flink

To run AthenaX, you need to build both AthenaX and Flink. They require Java 8 and Maven 3 to be built.

### Build AthenaX

```bash
$ git clone git@github.com:uber/AthenaX.git
$ mvn clean install
```

### Build Flink

```bash
$ git clone git@github.com:apache/flink.git
$ mvn clean install
```

## Configuring AthenaX

The next step is to write a YAML-based configuration file. Here is an example:

```yaml
athenax.master.uri: http://localhost:8083
catalog.impl: com.foo.MyCatalogProvider
clusters:
  foo:
    yarn.site.location: hdfs:///app/athenax/yarn-site.xml
    athenax.home.dir: hdfs:///tmp/athenax
    filnk.uber.jar.location: hdfs:///app/athenax/flink.jar
    localize.resources:
      - http://foo/log4j.properties
    additional.jars:
      - http://foo/connectors.jar
      - http://foo/foo.jar
extras:
  foo: bar
```

The meanings of the configuration are the following:

Entry                   | Required | Description
----------------------- | -------- | -----------  
athenax.master.uri      | Yes      | The REST endpoint that the AthenaX master should listen to.
catalog.impl            | Yes      | The class name of your [catalog provider](https://github.com/uber/AthenaX/blob/master/athenax-vm-api/src/main/java/com/uber/athenax/vm/api/AthenaXTableCatalogProvider.java).
clusters                | Yes      | Describe the YARN cluster each of which is a sub-entry of the configuration.
yarn.site.location      | Yes      | The location of the `yarn-site.xml` that contains the Hadoop-specific configuration of the cluster. Will be used by all instances in the cluster.
athenax.home.dir        | Yes      | A temporary directory used when starting all instances.
filnk.uber.jar.location | Yes      | The location of the Flink uber JAR that is generated from the previous step.
localize.resources      | Yes      | Additional files that will be localized and shipped along with all job instances but will not be added into the classpaths of the instances (e.g., `log4j.properties`).
additional.jars         | Yes      | Additional JARs that will be localized and added into the classpaths of the instances (e.g., the JARs of the connectors and their dependency).
extras                  | Yes      | Additional configuration that can be used by your customization.

Please see [AthenaXConfiguration](https://github.com/uber/AthenaX/blob/master/athenax-backend/src/main/java/com/uber/athenax/backend/server/AthenaXConfiguration.java) for more details on the configuration.

## Start

```bash
$ java -jar athenax-backend-0.1-SNAPSHOT.jar --conf <your configuration>
```

AthenaX will start serving the [REST API](https://github.com/uber/AthenaX/blob/master/athenax-backend/src/main/resources/athenax-backend-api.yaml) on the configured endpoint. Users can start submitting jobs.

## Customizing AthenaX

AthenaX is designed to be pluggable in order to satisfy the needs in different contexts. The [catalog](https://github.com/uber/AthenaX/blob/master/athenax-vm-api/src/main/java/com/uber/athenax/vm/api/AthenaXTableCatalogProvider.java), [connectors](https://github.com/uber/AthenaX/blob/master/athenax-vm-api/src/main/java/com/uber/athenax/vm/api/DataSinkProvider.java), [data store](https://github.com/uber/AthenaX/blob/master/athenax-backend/src/main/java/com/uber/athenax/backend/server/jobs/JobStore.java), and the [watchdog](https://github.com/uber/AthenaX/blob/master/athenax-backend/src/main/java/com/uber/athenax/backend/server/jobs/WatchdogPolicy.java) are all pluggable. Please refer to the respective implementation for more details.
