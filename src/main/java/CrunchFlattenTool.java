import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.View;
import org.kitesdk.data.crunch.CrunchDatasets;

public class CrunchFlattenTool extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    String inputPath = args[0];
    String outputPath = args[1];

    Configuration conf = getConf();

    Path path = new Path(inputPath);
    Schema schema = SchemaUtils.readSchema(path, conf);
    Source source = SchemaUtils.readSource(path, conf, schema);

    Schema flatSchema = new Flattener().flatten(schema);

    Pipeline pipeline = new MRPipeline(CrunchFlattenTool.class, conf);
    PCollection<GenericData.Record> records = pipeline.read(source);

    PCollection<GenericData.Record> flatRecords = records.parallelDo(new
        FlattenFn(flatSchema), Avros.generics(flatSchema));

    DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(flatSchema)
        .format(Formats.PARQUET)
        .compressionType(CompressionType.Uncompressed)
        .build();

    View<GenericData.Record> dataset = Datasets.create("dataset:" + outputPath, desc,
        GenericData.Record.class);

    pipeline.write(flatRecords, CrunchDatasets.asTarget(dataset));

    PipelineResult result = pipeline.done();
    return result.succeeded() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new CrunchFlattenTool(), args);
    System.exit(exitCode);
  }

  private static class FlattenFn extends DoFn<GenericData.Record, GenericData.Record> {

    private String schemaString;
    private transient Schema flatSchema;
    private transient Flattener flattener;

    public FlattenFn(Schema flatSchema) {
      this.schemaString = flatSchema.toString();
    }

    @Override
    public void initialize() {
      flatSchema = Schema.parse(schemaString);
      flattener = new Flattener();
    }

    @Override
    public void process(GenericData.Record input, Emitter<GenericData.Record> emitter) {
      emitter.emit(flattener.flattenRecord(flatSchema, input));
    }
  }
}
