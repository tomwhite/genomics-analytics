import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.crunch.Source;
import org.apache.crunch.io.From;
import org.apache.crunch.io.parquet.AvroParquetFileSource;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.spi.Schemas;

public class SchemaUtils {
  private SchemaUtils() {
  }

  private static Format readFormat(Path path) {
    if (path.getName().endsWith(".avro")) {
      return Formats.AVRO;
    } else if (path.getName().endsWith(".parquet")) {
      return Formats.PARQUET;
    }
    throw new IllegalStateException("Unrecognized format for " + path);
  }

  private static Path findFile(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (fs.isDirectory(path)) {
      FileStatus[] fileStatuses = fs.listStatus(path, new PathFilter() {
        @Override
        public boolean accept(Path p) {
          String name = p.getName();
          return !name.startsWith("_") && !name.startsWith(".");
        }
      });
      return fileStatuses[0].getPath();
    } else {
      return path;
    }
  }

  public static Schema readSchema(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    Path file = findFile(path, conf);
    Format format = readFormat(file);
    if (format == Formats.AVRO) {
      return Schemas.fromAvro(fs, file);
    } else if (format == Formats.PARQUET) {
      return Schemas.fromParquet(fs, file);
    }
    throw new IllegalStateException("Unrecognized format for " + file);
  }

  public static Source readSource(Path path, Configuration conf, Schema schema) throws IOException {
    Path file = findFile(path, conf);
    Format format = readFormat(file);
    if (format == Formats.AVRO) {
      return From.avroFile(path);
    } else if (format == Formats.PARQUET) {
      return new AvroParquetFileSource(path, Avros.generics(schema));
    }
    throw new IllegalStateException("Unrecognized format for " + file);
  }
}
