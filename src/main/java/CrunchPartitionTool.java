import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.View;
import org.kitesdk.data.crunch.CrunchDatasets;
import org.kitesdk.data.spi.PartitionStrategyParser;

public class CrunchPartitionTool extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    String partitionStrategyName = args[0];
    String inputPath = args[1];
    String outputPath = args[2];

    Configuration conf = getConf();

    Path path = new Path(inputPath);
    Schema schema = SchemaUtils.readSchema(path, conf);
    Source source = SchemaUtils.readSource(path, conf, schema);

    Pipeline pipeline = new MRPipeline(CrunchPartitionTool.class, conf);
    PCollection<GenericData.Record> records = pipeline.read(source);

    DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(schema)
        .partitionStrategy(readPartitionStrategy(partitionStrategyName))
        .format(Formats.PARQUET)
        .compressionType(CompressionType.Uncompressed)
        .build();

    View<GenericData.Record> dataset = Datasets.create("dataset:" + outputPath, desc,
        GenericData.Record.class);

    int numReducers = conf.getInt("mapreduce.job.reduces", 1);
    System.out.println("Num reducers: " + numReducers);
    PCollection<GenericData.Record> partition =
        CrunchDatasets.partition(records, dataset, numReducers);

    pipeline.write(partition, CrunchDatasets.asTarget(dataset));

    PipelineResult result = pipeline.done();
    return result.succeeded() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new CrunchPartitionTool(), args);
    System.exit(exitCode);
  }

  private PartitionStrategy readPartitionStrategy(String name) throws IOException {
    InputStream in = CrunchPartitionTool.class.getResourceAsStream(name + ".json");
    try {
      return PartitionStrategyParser.parse(in);
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }
}
