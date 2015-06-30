# GA4GH Analytics

Demonstrating how to do analytics on variants data stored in GA4GH Parquet format.

## Pre-requisites and tool installation

You will need a Hadoop cluster. These instructions assume CDH 5.4.2.

We'll use the Kite command line tools. Checkout, build and install this version, which
fixes a bug in the Hive partitioning: [https://github.com/tomwhite/kite/tree/existing-partition-bug](https://github.com/tomwhite/kite/tree/existing-partition-bug)

Unpack the Kite tarball and put the directory on your `PATH`.

```bash
export PATH=~/sw/kite-tools-cdh5-1.1.1-SNAPSHOT/bin:$PATH
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HIVE_CONF_DIR=/etc/hive/conf
```

## Getting data

We'll start with a test file that is already in GA4GH Avro format. This was generated
using [hpg-bigdata](https://github.com/opencb/hpg-bigdata).

First convert it to Avro with no compression:

```bash
avro-tools recodec ~/data/isaac2.vcf.gz.avro.deflate ~/data/isaac2.vcf.gz.avro
```

Then copy it to HDFS:

```bash
hadoop fs -mkdir -p datasets/variants_avro
hadoop fs -put ~/data/isaac2.vcf.gz.avro datasets/variants_avro
```

## Creating a dataset

First we convert the data into a Kite dataset, which makes it easier to work on it with
 Hadoop tools. The following command converts it in-place, by adding metadata to a
 _.metadata_ directory:

```bash
kite-dataset create dataset:hdfs:datasets/variants_avro
```

## Flattening

The GA4GH schemas have nested fields that don't work with Hive and Impala, so we need
to flatten nested fields and remove unsupported fields (like arrays). We do this using a
Crunch program, which will flatten any Avro or Parquet-formatted data and write it out
in Parquet format.

```bash
hadoop jar target/genomics-analytics-0.0.1-SNAPSHOT-job.jar \
  CrunchFlattenTool \
  datasets/variants_avro \
  hdfs:datasets/variants_flat
```

Inspect the data with

```bash
kite-dataset show dataset:hdfs:datasets/variants_flat
```

### Using Hive and Impala

To run SQL queries on the data we need to have the metadata in the Hive metastore. One
way of doing that is to create an external table. Kite can do this withe the following
command:

```bash
kite-dataset create dataset:hive:/user/tom/datasets/variants_flat
```

Try querying the table in Hive:

```bash
hive -e 'select count(*) from datasets.variants_flat'
```

Or Impala:

```bash
impala-shell -q 'invalidate metadata'
impala-shell -q 'compute stats datasets.variants_flat'
impala-shell -q 'select count(*) from datasets.variants_flat'
```

## Partitioning

The existing data is not partitioned. We can use another Crunch program to partition it
 and write the new partitioned data into a new dataset with a particular partitioning
 strategy (specified in a JSON file). We partition by chromosome and locus rounded down
  to the nearest million.

```bash
hadoop jar target/genomics-analytics-0.0.1-SNAPSHOT-job.jar \
  CrunchPartitionTool \
  ga4gh-variants-partition-strategy \
  datasets/variants_flat \
  hdfs:datasets/variants_flat_locuspart
```

### Using Hive and Impala

Use Kite to create a Hive table for the data. Note that this command takes several
minutes to run since partitions are added one-by-one rather than in batches:

```bash
kite-dataset create dataset:hive:/user/tom/datasets/variants_flat_locuspart
```

Try querying the table in Hive:

```bash
hive -e 'select count(*) from datasets.variants_flat_locuspart'
```

Or Impala:

```bash
impala-shell -q 'invalidate metadata'
impala-shell -q 'compute stats datasets.variants_flat_locuspart'
impala-shell -q 'select count(*) from datasets.variants_flat_locuspart'
impala-shell -q 'select count(*) from datasets.variants_flat_locuspart where referencename="chr1"'
```

The following expression can be used in SQL queries to find the locus segment (`pos`)
that a particular locus in the sequence (`<locus>`) occurs in.

```
cast(floor(<locus> / 1000000.) AS INT) * 1000000
```

