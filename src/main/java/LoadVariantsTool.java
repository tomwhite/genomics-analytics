import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.View;
import org.kitesdk.data.crunch.CrunchDatasets;

/**
 * Loads Variants stored in Avro or Parquet GA4GH format into a Hadoop filesystem,
 * ready for querying with Hive or Impala.
 */
public class LoadVariantsTool extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: " + getClass().getSimpleName() +
          " <partition-strategy> <sample-group> <input-path> <output-path>");
      System.exit(1);
    }
    String partitionStrategyName = args[0];
    String sampleGroup = args[1];
    String inputPath = args[2];
    String outputPath = args[3];

    Configuration conf = getConf();

    Path path = new Path(inputPath);
    Schema schema = SchemaUtils.readSchema(path, conf);
    Schema flatSchema = new Flattener().flatten(schema);
    Source source = SchemaUtils.readSource(path, conf, schema);

    Pipeline pipeline = new MRPipeline(CrunchPartitionTool.class, conf);
    PCollection<GenericData.Record> records = pipeline.read(source);

    PCollection<GenericData.Record> flatRecords = records.parallelDo(
        new CrunchFlattenTool.FlattenFn(flatSchema), Avros.generics(flatSchema));

    DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(schema)
        .partitionStrategy(CrunchPartitionTool.readPartitionStrategy(partitionStrategyName))
        .format(Formats.PARQUET)
        .compressionType(CompressionType.Uncompressed)
        .build();

    View<GenericData.Record> dataset = Datasets.create(outputPath, desc,
        GenericData.Record.class).getDataset().with("sample_group", sampleGroup);

    int numReducers = conf.getInt("mapreduce.job.reduces", 1);
    System.out.println("Num reducers: " + numReducers);
    PCollection<GenericData.Record> partition =
        CrunchDatasets.partition(flatRecords, dataset, numReducers);

    PTable<String, GenericData.Record> keyed = partition.by(
        new CrunchPartitionTool.ExtractSortKeyFn(), Avros.strings());
    PCollection<GenericData.Record> sorted = Sort.sort(keyed).values();

    pipeline.write(sorted, CrunchDatasets.asTarget(dataset));

    PipelineResult result = pipeline.done();
    return result.succeeded() ? 0 : 1;

  }

}
