# ADAM Analytics

Demonstrating how to do analytics on ADAM variants data.

## Install tools

First install ADAM

```bash
# git clone https://github.com/bigdatagenomics/adam.git
git clone https://github.com/tomwhite/adam.git # for https://github.com/bigdatagenomics/adam/pull/630
cd adam
export MAVEN_OPTS='-Xmx512m -XX:MaxPermSize=128m'
mvn clean package -DskipTests
```

Then install Parquet tools (not needed if you are using CDH).

```bash
curl -s -L -O http://search.maven.org/remotecontent?filepath=com/twitter/parquet-tools/1.6.0rc3/parquet-tools-1.6.0rc3-bin.tar.gz
tar zxf parquet-tools-*-bin.tar.gz
```

And [Kite tools](http://kitesdk.org/docs/1.0.0/Install-Kite.html) (not needed if you are using CDH).

## Set up the environment

```bash
export ADAM_HOME=~/adam
export SPARK_HOME=/opt/cloudera/parcels/CDH-5.3.0-1.cdh5.3.0.p0.30/lib/spark 
export PARQUET_HOME=/opt/cloudera/parcels/CDH-5.3.0-1.cdh5.3.0.p0.30/lib/parquet
export PATH=$PATH:$ADAM_HOME/bin:$PARQUET_HOME/bin
```

## Copy some genomics data to the cluster

We'll start with just the chr22 VCF from 1000 genomes:

```bash
curl -s -L ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
 | gunzip \
 | hadoop fs -put - genomics/1kg/vcf/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf
```

Convert it to Parquet using ADAM. Note that the number of executors should reflect the size of your cluster.

```bash
adam-submit --master yarn-cluster --driver-memory 4G --num-executors 24 --executor-cores 2 --executor-memory 4G \
  vcf2adam  genomics/1kg/vcf/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf genomics/1kg/parquet/chr22  
```

Flatten with ADAM, so that we can use Impala to query later on.

```bash
adam-submit --master yarn-cluster --driver-memory 4G --num-executors 24 --executor-cores 2 --executor-memory 4G \
  flatten genomics/1kg/parquet/chr22 genomics/1kg/parquet/chr22_flat
```
## Register the data in the Hive metastore

This is most easily done using the Kite tools.

First retrieve the flattened schema from one of the files:

```bash
parquet-schema hdfs://bottou01-10g.pa.cloudera.com/user/tom/genomics/1kg/parquet/chr22_flat/part-r-00001.gz.parquet | grep 'extra:' meta.txt
```

Copy the value of `avro.schema` into a new file called genotype_flat.avsc. And then store it in HDFS (we need to do this so that the schema is stored as a reference to a file in HDFS, rather than its literal value, since the latter is too large for the Hive metastore):

```bash
hadoop fs -mkdir schemas
hadoop fs -put genotype_flat.avsc schemas/genotype_flat.avsc
```

Create the Kite dataset in Hive:

```bash
kite-dataset delete dataset:hive:genotypes
kite-dataset create dataset:hive:genotypes --schema hdfs://bottou01-10g.pa.cloudera.com/user/tom/schemas/genotype_flat.avsc \
  --format parquet
```

Move the data into the Kite dataset:

```bash
hadoop fs -mv genomics/1kg/parquet/chr22_flat/*.parquet /user/hive/warehouse/genotypes
```

## Use Impala to query the dataset

Start an Impala shell by typing `impala-shell`. Then run the following so that Impala picks up the new data, and then computes statistics on it.

```sql
invalidate metadata;
compute stats genotypes;
```

Try some queries:
```sql
select count(*) from genotypes;
select * from genotypes limit 1;
select count(*) from genotypes where variant__start > 50000000 and variant__end < 51000000;
select min(variant__start),  max(variant__start) from genotypes;
```


