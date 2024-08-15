# EVTX Parser

A Spark custom datasource reader to parse EVTX files.

## Overview
This project provides a custom datasource reader for Apache Spark to parse EVTX files. EVTX files are used by Windows to store event logs, and this project allows you to read and process these files using Spark.

## building the JAR File

### Step 1: Clone the Project
Clone this project and import it into IntelliJ.

### Step 2: Build the JAR File
Open the Maven project and navigate to `spark-evtx` > `Lifecycle` > `package`.

## Running the JAR File

### Command line

```bash
spark-shell --jars spark-evtx-1.0-SNAPSHOT.jar
```

### Databricks environment

1. Import the JAR file into your workspace.
2. Add the JAR file to your cluster's library.

## Some code examples

### using readStream method:

#### Define the Schema

```java
import org.apache.spark.sql.types._

val schema = StructType(Array(
StructField("System", StructType(Array(
StructField("Provider", StructType(Array(
StructField("Name", StringType, true)
)), true),
StructField("EventID", StructType(Array(
StructField("Qualifiers", IntegerType, true),
StructField("EventID", IntegerType, true)
)), true),
StructField("Version", IntegerType, true),
StructField("Level", IntegerType, true),
StructField("Task", IntegerType, true),
StructField("Opcode", IntegerType, true),
StructField("Keywords", StringType, true),
StructField("TimeCreated", StructType(Array(
StructField("SystemTime", TimestampType, true)
)), true),
StructField("EventRecordID", IntegerType, true),
StructField("Correlation", StructType(Array(
StructField("ActivityID", StringType, true),
StructField("RelatedActivityID", StringType, true)
)), true),
StructField("Execution", StructType(Array(
StructField("ProcessID", IntegerType, true),
StructField("ThreadID", IntegerType, true)
)), true),
StructField("Channel", StringType, true),
StructField("Computer", StringType, true),
StructField("Security", StructType(Array(
StructField("UserID", StringType, true)
)), true)
)), true),
StructField("EventData", StructType(Array(
StructField("Data", StringType, true),
StructField("Binary", StringType, true)
)), true),
StructField("xmlns", StringType, true)
))
```

#### Create a DataFrame using ReadStream

```java
val df = spark.readStream.format("br.com.brainboss.evtx.datasource.EVTX")
  .schema(schema)
  .option("numPartitions", 1000)
  .load("/root/")
```

#### Write the DataFrame to the Console

```java 
df.writeStream.format("console")
  .outputMode("append")
  .start()
  .awaitTermination()

```

### Using Read Method:

#### Define the Schema

```java
import org.apache.spark.sql.types._

val schema = StructType(Array(
  StructField("System", StructType(Array(
    StructField("Provider", StructType(Array(
      StructField("Name", StringType, true)
    )), true),
    StructField("EventID", StructType(Array(
      StructField("Qualifiers", IntegerType, true),
      StructField("EventID", IntegerType, true)
    )), true),
    StructField("Version", IntegerType, true),
    StructField("Level", IntegerType, true),
    StructField("Task", IntegerType, true),
    StructField("Opcode", IntegerType, true),
    StructField("Keywords", StringType, true),
    StructField("TimeCreated", StructType(Array(
      StructField("SystemTime", TimestampType, true)
    )), true),
    StructField("EventRecordID", IntegerType, true),
    StructField("Correlation", StructType(Array(
      StructField("ActivityID", StringType, true),
      StructField("RelatedActivityID", StringType, true)
    )), true),
    StructField("Execution", StructType(Array(
      StructField("ProcessID", IntegerType, true),
      StructField("ThreadID", IntegerType, true)
    )), true),
    StructField("Channel", StringType, true),
    StructField("Computer", StringType, true),
    StructField("Security", StructType(Array(
      StructField("UserID", StringType, true)
    )), true)
  )), true),
  StructField("EventData", StructType(Array(
    StructField("Data", StringType, true),
    StructField("Binary", StringType, true)
  )), true),
  StructField("xmlns", StringType, true)
))
```

#### Create a DataFrame using Read

```java
val df = spark.read.format("br.com.brainboss.evtx.datasource.EVTX")
  .schema(schema)
  .option("fileName", "/root/Application_4G.evtx")
  .option("numPartitions", 1000)
  .load("/root/Application_4G.evtx")
```

#### Print the DataFrame
```java
df.count
df.show
```

## Options

### Enabling Debug Mode

To enable debug mode, add the following option when creating the DataFrame

When debugMode is enabled, the schema is bypassed to prevent errors that may occur when attempting to structure data in a schema format.

```java
val df = spark.readStream.format("br.com.brainboss.evtx.datasource.EVTX")
  .schema(schema)
  .option("debugMode", true)
  .option("numPartitions", 1000)
  .load("/root/")
```



