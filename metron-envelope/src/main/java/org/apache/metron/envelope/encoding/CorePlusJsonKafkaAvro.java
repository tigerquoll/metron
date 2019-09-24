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

/**
 * A Spark Row Encoding Strategy that:
 *  - Encodes all Metron data into multiple well defined fields plus one combined field of everything else
 *  - The not well-defined Metron data is serialized via JSON into a String field
 *  - Metron data is encoded into Kafka via Avro encoding all the fields
 */
public class CorePlusJsonKafkaAvro extends AbstractSparkRowEncodingStrategy implements SparkRowEncodingStrategy {
  private static final String AVRO_SCHEMA_NAME = "CorePlusJsonKafkaAvro1";

  @Override
  public void init() {
    super.init(new JsonFactory(),
            DataFieldType.FieldType_String,
            KafkaSerializationType.avro,
            FieldLayoutType.corePlus,
            AVRO_SCHEMA_NAME);
  }

}
