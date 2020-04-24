/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.v2;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class EcoKafkaAvroDecoder extends AbstractKafkaAvroDeserializer
    implements Decoder<Object>, Deserializer<Object> {

  protected VerifiableProperties props;

  public EcoKafkaAvroDecoder() {
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    configure(deserializerConfig(configs));
  }

  @Override
  public Object deserialize(String topic, byte[] data) {
    return fromBytes(data);
  }

  @Override
  public Object fromBytes(byte[] bytes) {
    Object decoded = null;

    try {
      decoded = deserialize(bytes);
    } catch (SerializationException e) {
      if (bytes.length == 4) { //int key
        decoded = ByteBuffer.wrap(bytes).getInt();
      } else if (bytes.length == 8) { //long key
        decoded = ByteBuffer.wrap(bytes).getLong();
      } else {
        decoded = new StringDecoder(props).fromBytes(bytes);
      }
    }

    return decoded;
  }

  @Override
  public void close() {

  }
}