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

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.fasterxml.jackson.core.JsonProcessingException;
import envelope.shaded.com.google.common.collect.ImmutableList;
import org.apache.metron.common.error.MetronError;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONObject;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Combined strategies encode all values into a single field
 */
public abstract class AbstractCombined extends AbstractSparkRowEncodingStrategy implements SparkRowEncodingStrategy {
  @Override
  protected List<StructField> getParserOutputSparkSchemaBaseTemplate() {
    // Combined Strategies combine all fields into a single field,
    // so there are no additional sparkfields except for the primary data field
    return Collections.emptyList();
  }

  @Override
  public RowWithSchema encodeParserErrorIntoSparkRow(@NotNull MetronError metronError) throws JsonProcessingException {
    final Object combinedFieldValue = Objects.requireNonNull(EncodingUtils.encodeCombinedFields(getMapper(), getDataFieldType(), metronError.getJSONObject()));
    return new RowWithSchema(getParserSparkOutputSchema(), ImmutableList.of(combinedFieldValue));
  }

  @Override
  public RowWithSchema encodeParserResultIntoSparkRow(@NotNull JSONObject parsedMessage) throws JsonProcessingException {
    final Object combinedFieldValue = Objects.requireNonNull(EncodingUtils.encodeCombinedFields(getMapper(), getDataFieldType(), parsedMessage));
    return new RowWithSchema(getParserSparkOutputSchema(), ImmutableList.of(combinedFieldValue));
  }

  @Override
  public JSONObject decodeParsedMessage(@NotNull Row row) {
    return null;
  }
}
