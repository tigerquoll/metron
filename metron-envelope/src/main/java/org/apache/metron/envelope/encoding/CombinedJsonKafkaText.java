/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.metron.envelope.encoding;

import com.fasterxml.jackson.core.JsonFactory;
import org.jetbrains.annotations.Nullable;

/**
 * A Spark Row Encoding Strategy that:
 * - Encodes all Metron data into a single field
 * - Metron Data is Serialized via JSON into a text field
 * - When passed over Kafka, Metron Data is encoded into the Kafka message body as a single JSON String
 */
public class CombinedJsonKafkaText extends AbstractCombined implements SparkRowEncodingStrategy {
  @Override
  public void init(@Nullable String additionalConfig) {
    super.init(new JsonFactory(), DataFieldType.FieldType_String, KafkaSerializationType.text, additionalConfig);
  }

  @Override
  protected String getAvroSchemaName() {
    throw new UnsupportedOperationException("CombinedJsonKafkaText SparkRowEncoder cannot be used with Avro");
  }
}
