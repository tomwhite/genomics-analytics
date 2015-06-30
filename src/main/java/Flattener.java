/*
 * Copyright 2015 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

public class Flattener {

  private static final String SEPARATOR = "__";
  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

  public Schema flatten(Schema schema) {
    Schema flatSchema = Schema.createRecord(schema.getName(), schema.getDoc(),
        schema.getNamespace(), schema.isError());
    flatSchema.setFields(flatten(schema, "", new ArrayList<Schema.Field>(), false));
    return flatSchema;
  }

  private List<Schema.Field> flatten(Schema schema, String prefix, List<Schema.Field>
      accumulator, boolean makeOptional) {
    for (Schema.Field f : schema.getFields()) {
      switch (f.schema().getType()) {
        case NULL:
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BYTES:
        case STRING:
        case FIXED:
        case ENUM:
          accumulator.add(copy(f, prefix, makeOptional));
          break;
        case RECORD:
          flatten(f.schema(), prefix + f.name() + SEPARATOR, accumulator, makeOptional);
          break;
        case UNION:
          List<Schema> types = f.schema().getTypes();
          if ((types.size() == 2) && types.contains(NULL_SCHEMA)) {
            Schema s = types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0);
            switch (s.getType()) {
              case NULL:
              case BOOLEAN:
              case INT:
              case LONG:
              case FLOAT:
              case DOUBLE:
              case BYTES:
              case STRING:
              case FIXED:
              case ENUM:
                accumulator.add(copy(f, prefix, makeOptional));
                break;
              case RECORD:
                boolean opt = makeOptional || f.defaultValue().equals(NullNode.getInstance());
                flatten(s, prefix + f.name() + SEPARATOR, accumulator, opt);
                break;
              case UNION:
              case ARRAY:
              case MAP:
              default:
                // drop field
                break;
            }
          }
          break;
        case ARRAY:
        case MAP:
        default:
          // drop field
          break;
      }
    }
    return accumulator;
  }

  private static Schema.Field copy(Schema.Field f, String prefix, boolean makeOptional) {
    Schema schema = makeOptional ? makeOptional(f.schema()) : f.schema();
    JsonNode def = (f.defaultValue() == null && makeOptional) ? NullNode.getInstance()
        : f.defaultValue();
    Schema.Field copy = new Schema.Field(prefix + f.name(), schema, f.doc(), def);
    // retain mapping properties, note that this needs to use the Jackson 1 API until
    // we can use AVRO-1585
    for (Map.Entry<String, JsonNode> prop : f.getJsonProps().entrySet()) {
      copy.addProp(prop.getKey(), prop.getValue());
    }
    return copy;
  }

  private static Schema makeOptional(Schema schema) {
    if (schema.getType() == Schema.Type.NULL) {
      return schema;
    }

    if (schema.getType() != Schema.Type.UNION) {
      return Schema.createUnion(ImmutableList.of(NULL_SCHEMA, schema));
    }

    return schema; // TODO: need to consider other types of unions
  }

  public GenericData.Record flattenRecord(Schema flatSchema, GenericData.Record record) {
    GenericData.Record flatRecord = new GenericData.Record(flatSchema);
    flatten(record.getSchema(), record, flatRecord, 0);
    return flatRecord;
  }

  private int flatten(Schema schema, IndexedRecord record, IndexedRecord
      flatRecord, int offset) {
    if (record == null) {
      return offset + schema.getFields().size();
    }
    for (Schema.Field f : record.getSchema().getFields()) {
      switch (f.schema().getType()) {
        case NULL:
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BYTES:
        case STRING:
        case FIXED:
        case ENUM:
          flatRecord.put(offset++, record.get(f.pos()));
          break;
        case RECORD:
          offset = flatten(f.schema(), (IndexedRecord) record.get(f.pos()), flatRecord,
              offset);
          break;
        case UNION:
          List<Schema> types = f.schema().getTypes();
          if ((types.size() == 2) && types.contains(NULL_SCHEMA)) {
            Schema s = types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0);
            switch (s.getType()) {
              case NULL:
              case BOOLEAN:
              case INT:
              case LONG:
              case FLOAT:
              case DOUBLE:
              case BYTES:
              case STRING:
              case FIXED:
              case ENUM:
                flatRecord.put(offset++, record.get(f.pos()));
                break;
              case RECORD:
                offset = flatten(s, (IndexedRecord) record.get(f.pos()), flatRecord,
                    offset);
                break;
              case UNION:
              case ARRAY:
              case MAP:
              default:
                // drop field
                break;
            }
          }
          break;
        case ARRAY:
        case MAP:
        default:
          // drop field
          break;
      }
    }
    return offset;
  }

}
