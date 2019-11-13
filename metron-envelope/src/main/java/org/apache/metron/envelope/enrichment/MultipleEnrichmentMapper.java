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
package org.apache.metron.envelope.enrichment;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.typesafe.config.Config;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.metron.enrichment.cache.CacheKey;
import org.apache.metron.enrichment.interfaces.EnrichmentAdapter;
import org.apache.metron.enrichment.parallel.EnrichmentStrategies;
import org.apache.metron.envelope.adaptors.BaseMapper;
import org.apache.metron.envelope.utils.Either;
import org.apache.metron.envelope.utils.MetronErrorProcessor;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.metron.envelope.utils.ClassUtils.instantiateClass;
import static org.apache.metron.envelope.utils.ErrorUtils.convertErrorsToMetronErrors;
import static org.apache.metron.envelope.utils.ErrorUtils.convertSparkErrorsToMetronErrors;

/**
 * Provides the business logic of enriching messages
 */
public class MultipleEnrichmentMapper extends BaseMapper implements AutoCloseable {
  private static final LazyLogger LOG = LazyLoggerFactory.getLogger(MultipleEnrichmentMapper.class);
  private static final String ENRICHMENT_STRATEGY = "enrichmentStrategy";
  transient private Map<String, EnrichmentAdapter<CacheKey>> enrichmentsByType = null;
  transient private EnrichmentStrategies enrichmentStrategy;
  // Either enrich or threatintel
  private MetronErrorProcessor metronErrorProcessor;

  public MultipleEnrichmentMapper() {
  }

  /**
   * THe Constructor is typically called in the context of the driver.
   * It will store the reference to the configuration data so that is will be
   * serialised and passed to the worker, we it can then be processed
   * @param workerConfigBroadcast JSON String containing config for enrichments
   */
  public MultipleEnrichmentMapper(Broadcast<String> workerConfigBroadcast)  {
    super(workerConfigBroadcast);
  }

  @Override
  protected Config init() throws IOException {
    Config config = super.init();
    metronErrorProcessor = new MetronErrorProcessor(kafkaBrokers);

    String enrichmentStrategyName = Objects.requireNonNull(config.getString(MultipleEnrichmentMapper.ENRICHMENT_STRATEGY), "EnrichmentStrategy required");
    enrichmentStrategy = EnrichmentStrategies.valueOf(enrichmentStrategyName);
    // read in Enrichment Configuration:
    // enrichments are defined as
    // enrichments : [
    // { class: 'a.b.c.d.enrichmentClass1', config : { 'var1': value1, 'var2': value2} }
    // { class: 'a.b.c.d.enrichmentClass2', config : { 'var1': value1, 'var2': value2} }
    // ]
    for(Config enrichmentConfig: config.getConfigList("enrichments")) {
      String enrichmentClass = enrichmentConfig.getString("class");
      EnrichmentAdapter<CacheKey> enrichment = (EnrichmentAdapter<CacheKey>)instantiateClass(enrichmentClass);
      Objects.requireNonNull(enrichment);
      Map<String,Object> enrichmentConfiguration = enrichmentConfig
              .getObject("config")
              .unwrapped()
              .entrySet()
              .stream()
              .collect(Collectors.toMap(Object::toString, Object::toString));
      enrichment.initializeAdapter(enrichmentConfiguration);
      // todo: confirm what the key supposed to be????
      enrichmentsByType.put(enrichment.getClass().getSimpleName(), enrichment);
    }
    return config;
  }
  /**
   * This function transforms input data by enriching it via configured Metron Enrichers
   * Configuration information is passed via Zookeeper, and json Broadcast
   * The call to this function should occur in the context of the spark worker nodes
   * @param input Parsed Metron data transformed to spark rows
   * @return Enriched spark rows
   * @throws Exception if an error occurs
   */
  @Override
  public Iterator<Row> call(Iterator<Row> input) throws Exception {
    if (initNeeded()) {
      init();
    }
    // Convert input iterator into a steam of spark input rows
    final Iterable<Row> iterable = () -> input;
    final Stream<Row> inputRowStream = StreamSupport.stream(iterable.spliterator(), false);

    // main processing loop
    Stream<RowWithSchema> serialisedMessages; // output var
    try (UnifiedSparkEnricher unifiedSparkEnricher = new UnifiedSparkEnricher(
            zookeeperQuorum,
            kafkaBrokers,
            encodingStrategy,
            enrichmentsByType,
            enrichmentStrategy,
            additionalConfig
            )) {
      // deserialize the message from spark back into a JSONObject
      Stream<JSONObject> deSerialisedMessages = inputRowStream
              .map(convertSparkErrorsToMetronErrors(enrichmentStrategy.getErrorType(), encodingStrategy::decodeParsedMessageFromKafka))
              .flatMap(x -> x.filterAndProcessErrors(metronErrorProcessor::handleMetronError));

      // Start enrichment activities
      List<Map.Entry<JSONObject, List<CompletableFuture<Either<MetronError, JSONObject>>>>> enrichmentPendingMessages = deSerialisedMessages
              // Store the original document as well as a list of pending enrichments
              .map(x -> new AbstractMap.SimpleEntry<>(x, unifiedSparkEnricher.asyncEnrich(x)))
              // Streams are lazy by default, this will force all documents in the stream to be processed
              .collect(Collectors.toList());

      // Extract and fold-in retrieved enrichment results, pass off errors to Kafka to process
      Stream<JSONObject> enrichedMessages = enrichmentPendingMessages.stream()
              .flatMap(MultipleEnrichmentMapper::processEnrichmentResults)
              .flatMap(x -> x.filterAndProcessErrors(metronErrorProcessor::handleMetronError));

      // Serialise message back into a spark row
      serialisedMessages = enrichedMessages
              .map(convertErrorsToMetronErrors(enrichmentStrategy.getErrorType(), encodingStrategy::encodeEnrichedMessageIntoSparkRow))
              .flatMap(x -> x.filterAndProcessErrors(metronErrorProcessor::handleMetronError));
    }
    // cast type to Interface type for compatibility with Spark
    return serialisedMessages.map( x -> (Row)x).iterator();
  } // call()

  /**
   * Extract the results of the lookups, merge in returned data,
   * @param asyncEnrichmentResults JSONObject to enrich + List of Future Enrichment requests
   * @return Stream of at least 1 Either.data and 0 or more Metron Errors wrapped in Either.error objects
   */
  private static Stream< Either<MetronError,JSONObject> > processEnrichmentResults(
          Map.Entry<JSONObject,List<CompletableFuture<Either<MetronError,JSONObject>>>> asyncEnrichmentResults) {
    final JSONObject messageToEnrich = asyncEnrichmentResults.getKey();
    final List<CompletableFuture<Either<MetronError,JSONObject>>> pendingEnrichments = asyncEnrichmentResults.getValue();

    Objects.requireNonNull(messageToEnrich, "Asked to enrich a null message");
    final List<Either<MetronError, JSONObject>> results = new ArrayList<>();

    pendingEnrichments.forEach( pendingEnrichment -> {
      try {
        pendingEnrichment.get().forEach(
                error -> results.add( Either.Error(error)),
                enrichments -> messageToEnrich.putAll(enrichments)
        );
      } catch (InterruptedException | ExecutionException e) {
        // If we get an exception here then something very unexpected has happened here,
        // as all exceptions should have been converted to MetronErrors by this point
        LOG.error("Exception caught unwrapping an enrichment future", e);
        results.add( Either.Error(new MetronError().withThrowable(e) ));
      }
    });
    // All enrichment results processed, wrap up enriched message and add it to result
    results.add( Either.Data(messageToEnrich) );
    return results.stream();
  }

  @Override
  public void close() {
    if (enrichmentsByType != null) {
      enrichmentsByType.forEach( (k,enrichmentAdapter) -> enrichmentAdapter.cleanup());
    }
    if (metronErrorProcessor != null) {
      metronErrorProcessor.close();
    }
  }
}
