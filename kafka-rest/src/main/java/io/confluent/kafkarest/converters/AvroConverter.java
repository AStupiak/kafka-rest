/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides conversion of JSON to/from Avro.
 */
public class AvroConverter {

  private static final Logger log = LoggerFactory.getLogger(AvroConverter.class);

  private static final EncoderFactory encoderFactory = EncoderFactory.get();
  private static final DecoderFactory decoderFactory = DecoderFactory.get();
  private static final ObjectMapper jsonMapper = new ObjectMapper();

  private static final Map<String, Schema> primitiveSchemas;

  static {
    Schema.Parser parser = new Schema.Parser();
    primitiveSchemas = new HashMap<String, Schema>();
    primitiveSchemas.put("Null", createPrimitiveSchema(parser, "null"));
    primitiveSchemas.put("Boolean", createPrimitiveSchema(parser, "boolean"));
    primitiveSchemas.put("Integer", createPrimitiveSchema(parser, "int"));
    primitiveSchemas.put("Long", createPrimitiveSchema(parser, "long"));
    primitiveSchemas.put("Float", createPrimitiveSchema(parser, "float"));
    primitiveSchemas.put("Double", createPrimitiveSchema(parser, "double"));
    primitiveSchemas.put("String", createPrimitiveSchema(parser, "string"));
    primitiveSchemas.put("Bytes", createPrimitiveSchema(parser, "bytes"));
  }

  public static Object toAvro(JsonNode value, Schema schema) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      jsonMapper.writeValue(out, value);
      DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
      Object object = reader.read(
          null, decoderFactory.jsonDecoder(schema, new ByteArrayInputStream(out.toByteArray())));
      out.close();
      return object;
    } catch (IOException | RuntimeException e) {
      // These can be generated by Avro's JSON decoder, the input/output stream operations, and the
      // Jackson ObjectMapper.writeValue call.
      throw new ConversionException("Failed to convert JSON to Avro: " + e.getMessage());
    }
  }

  public static class JsonNodeAndSize {

    public JsonNode json;
    public long size;

    public JsonNodeAndSize(JsonNode json, long size) {
      this.json = json;
      this.size = size;
    }
  }

  /**
   * Converts Avro data (including primitive types) to their equivalent JsonNode representation.
   *
   * @param value the value to convert
   * @return an object containing the root JsonNode representing the converted object and the size
   *     in bytes of the data when serialized
   */
  public static JsonNodeAndSize toJson(Object value) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Schema schema = getSchema(value);
      JsonEncoder encoder = encoderFactory.jsonEncoder(schema, out);
      DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
      // Some types require wrapping/conversion
      Object wrappedValue = value;
      if (value instanceof byte[]) {
        wrappedValue = ByteBuffer.wrap((byte[]) value);
      }
      writer.write(wrappedValue, encoder);
      encoder.flush();
      byte[] bytes = out.toByteArray();
      out.close();
      return new JsonNodeAndSize(jsonMapper.readTree(bytes), bytes.length);
    } catch (IOException e) {
      // These can be generated by Avro's JSON encoder, the output stream operations, and the
      // Jackson ObjectMapper.readTree() call.
      log.error("Jackson failed to deserialize JSON generated by Avro's JSON encoder: ", e);
      throw new ConversionException("Failed to convert Avro to JSON: " + e.getMessage());
    } catch (RuntimeException e) {
      // Catch-all since it's possible for, e.g., Avro to throw many different RuntimeExceptions
      log.error("Unexpected exception convertion Avro to JSON: ", e);
      throw new ConversionException("Failed to convert Avro to JSON: " + e.getMessage());
    }
  }

  // Gets the schema associated with the Avro object. This will either be extracted from the
  // GenericRecord or one of the primitive schemas
  private static Schema getSchema(Object value) {
    if (value instanceof GenericContainer) {
      return ((GenericContainer) value).getSchema();
    } else if (value instanceof Map) {
      // This case is unusual -- the schema isn't available directly anywhere, instead we have to
      // take get the value schema out of one of the entries and then construct the full schema.
      Map mapValue = ((Map) value);
      if (mapValue.isEmpty()) {
        // In this case the value schema doesn't matter since there is no content anyway. This
        // only works because we know in this case that we are only using this for conversion and
        // no data will be added to the map.
        return Schema.createMap(primitiveSchemas.get("Null"));
      }
      Schema valueSchema = getSchema(mapValue.values().iterator().next());
      return Schema.createMap(valueSchema);
    } else if (value == null) {
      return primitiveSchemas.get("Null");
    } else if (value instanceof Boolean) {
      return primitiveSchemas.get("Boolean");
    } else if (value instanceof Integer) {
      return primitiveSchemas.get("Integer");
    } else if (value instanceof Long) {
      return primitiveSchemas.get("Long");
    } else if (value instanceof Float) {
      return primitiveSchemas.get("Float");
    } else if (value instanceof Double) {
      return primitiveSchemas.get("Double");
    } else if (value instanceof String) {
      return primitiveSchemas.get("String");
    } else if (value instanceof byte[] || value instanceof ByteBuffer) {
      return primitiveSchemas.get("Bytes");
    }
    
    throw new ConversionException("Couldn't determine Schema from object");
  }

  private static Schema createPrimitiveSchema(Schema.Parser parser, String type) {
    String schemaString = String.format("{\"type\" : \"%s\"}", type);
    return parser.parse(schemaString);
  }
}
