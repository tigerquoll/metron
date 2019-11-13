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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.base.Joiner;
import org.apache.metron.common.configuration.enrichment.SensorEnrichmentConfig;
import org.apache.metron.common.configuration.enrichment.handler.ConfigHandler;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.performance.PerformanceLogger;
import org.apache.metron.common.utils.MessageUtils;
import org.apache.metron.enrichment.cache.CacheKey;
import org.apache.metron.enrichment.interfaces.EnrichmentAdapter;
import org.apache.metron.enrichment.parallel.EnrichmentStrategies;
import org.apache.metron.enrichment.parallel.EnrichmentStrategy;
import org.apache.metron.enrichment.parallel.WorkerPoolStrategies;
import org.apache.metron.enrichment.utils.EnrichmentUtils;
import org.apache.metron.envelope.utils.Either;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * This is an independent component which will accept a message and a set of enrichment adapters as well as a config which defines
 * how those enrichments should be performed and fully enrich the message.  The result will be the enriched message
 * unified together and a list of errors which happened.
 */
public class SparkParallelEnricher {
  /**
   * Each enrichment type has its own AsyncLoadingCache - this is static so the cache is shared between Spark Partitions
   */
  private static transient ConcurrentMap<EnrichmentAdapter<CacheKey>, AsyncLoadingCache<CacheKey, JSONObject>> enrichmentCaches =
          new ConcurrentHashMap<>();

  private Map<String, EnrichmentAdapter<CacheKey>> enrichmentsByType = new HashMap<>();
  private EnumMap<EnrichmentStrategies, CacheStats> cacheStats = new EnumMap<>(EnrichmentStrategies.class);

  /**
   * Construct a parallel enricher with a set of enrichment adapters associated with their enrichment types.
   * @param enrichmentsByType
   */
  public SparkParallelEnricher(Map<String, EnrichmentAdapter<CacheKey>> enrichmentsByType
                         , boolean logStats
                         , long maxCacheSize
                         , long maxTimeRetain
                         , boolean invalidateCacheOnReload
                         , int numThreads
                         , WorkerPoolStrategies poolStrategy
                         )
  {
    Objects.requireNonNull(poolStrategy);
    this.enrichmentsByType = enrichmentsByType;
    if(logStats) {
      for(EnrichmentStrategies s : EnrichmentStrategies.values()) {
        cacheStats.put(s, null);
      }
    }

    // build our async cache for each enrichment type
    for(EnrichmentAdapter<CacheKey> enrichmentAdaptor :enrichmentsByType.values()) {
      final Caffeine<Object, Object> cacheTemplate = Caffeine.newBuilder()
              .maximumSize(maxCacheSize)
              .expireAfterWrite(maxTimeRetain, TimeUnit.MINUTES);

      Supplier<AsyncLoadingCache<CacheKey, JSONObject>> cacheSupplier = () -> logStats ?
              cacheTemplate
                  .executor(poolStrategy.create(numThreads))
                  .recordStats()
                  .buildAsync(enrichmentAdaptor::enrich) :
              cacheTemplate
                  .executor(poolStrategy.create(numThreads))
                  .buildAsync(enrichmentAdaptor::enrich);

      // Make this cache global for this type of enrichment, so that it will cover enrichment
      // operations over multiple spark partitions
      if (invalidateCacheOnReload) {
        // Force creation of a new cache
        enrichmentCaches.compute(enrichmentAdaptor, (k, existingCache) -> cacheSupplier.get());
      } else {
        // Re-use existing cache if present, otherwise create
        enrichmentCaches.computeIfAbsent(enrichmentAdaptor, k -> cacheSupplier.get());
      }
    }
  }

  /**
   * Fully enriches a message.  Each enrichment is done in parallel via a threadpool.
   * Each enrichment is fronted with a LRU cache.
   *
   * @param message the message to enrich
   * @param strategy The enrichment strategy to use (e.g. enrichment or threat intel)
   * @param config The sensor enrichment config
   * @param perfLog The performance logger.  We log the performance for this call, the split portion and the enrichment portion.
   * @return the enrichment result
   */
  public List<CompletableFuture<Either<MetronError,JSONObject>>> apply(final JSONObject message
                         , final EnrichmentStrategies strategy
                         , final SensorEnrichmentConfig config
                         , final PerformanceLogger perfLog
                         ) {
    final List<CompletableFuture<Either<MetronError,JSONObject>>> pendingMessageEnrichments = new ArrayList<>();
    if(message == null) {
      return pendingMessageEnrichments;
    }
    final String sensorType = MessageUtils.getSensorType(message);
    // Split the message into individual tasks.
    //
    // A task will either correspond to an enrichment adapter or,
    // in the case of Stellar, a stellar subgroup.  The tasks will be grouped by enrichment type (the key of the
    //tasks map).  Each JSONObject will correspond to a unit of work.
    // re-using this for expediency - not really required for spark batch parallelism
    Map<String, List<JSONObject>> tasks = splitMessage( message, strategy, config);

    for(Map.Entry<String, List<JSONObject>> task : tasks.entrySet()) {
      // task is the list of enrichment tasks for the task.getKey() adapter
      final EnrichmentAdapter<CacheKey> adapter = enrichmentsByType.get(task.getKey());
      final String adapterPerfEndKey = "adapter." + adapter.getClass().getSimpleName().toLowerCase() + ".end.ts";
      final String taskKeyString = task.getKey();

      if(adapter == null) {
        throw new IllegalStateException("Unable to find an adapter for " + task.getKey()
                + ", possible adapters are: " + Joiner.on(",").join(enrichmentsByType.keySet()));
      }
      message.put("adapter." + adapter.getClass().getSimpleName().toLowerCase() + ".begin.ts", "" + System.currentTimeMillis());
      for(JSONObject m : task.getValue()) {
        /* now for each unit of work (each of these only has one element in them)
         * the key is the field name and the value is value associated with that field.
         *
         * In the case of stellar enrichment, the field name is the subgroup name or empty string.
         * The value is the subset of the message needed for the enrichment.
         *
         * In the case of another enrichment (e.g. hbase), the field name is the field name being enriched.
         * The value is the corresponding value.
         */
        for(Object o : m.keySet()) {
          final String field = (String) o;
          final Object value = m.get(o);
          if(value == null) {
            pendingMessageEnrichments.add(createNoopFutureResult(adapterPerfEndKey));
            continue;
          }
          final CacheKey cacheKey = new CacheKey(field, value, config);
          final String prefix = adapter.getOutputPrefix(cacheKey);

          // Asynchronously look up / cache result, convert returned data as configured and transform exceptions into MetronErrors
          CompletableFuture<Either<MetronError,JSONObject>> futureEnrichment = enrichmentCaches.get(adapter)
              .get(cacheKey)
              .thenApply( enrichedField -> {
                  // adjust/add prefix if required, wrap in Either Structure
                  JSONObject adjustedKeys = EnrichmentUtils.adjustKeys(new JSONObject(), enrichedField, cacheKey.getField(), prefix);
                  adjustedKeys.put(adapterPerfEndKey, "" + System.currentTimeMillis());
                  return Either.<MetronError,JSONObject>Data(adjustedKeys);
              })
              .exceptionally(exception ->
                  // Convert pending exceptions into MetronErrors
                  Either.Error(
                    new MetronError()
                    .withSensorType(Collections.singleton(sensorType))
                    .withMessage(strategy + " error with " + taskKeyString + " failed: " + exception.getMessage())
                  )
              );
          pendingMessageEnrichments.add(futureEnrichment);
        } // for each enrichment
      } // for each enrichment of the same type
    } // for each enrichment type
    return pendingMessageEnrichments;
  }

  @NotNull
  /**
   * Returns a no-op enrichment result wrapped up in a pretend Future, for those
   * cases where we already know there is not going to be a result
   * @param adapterPerfEndKey performance key
   * @return No-op enrichment result
   */
  private CompletableFuture<Either<MetronError, JSONObject>> createNoopFutureResult(String adapterPerfEndKey) {
    JSONObject noopEnrichmentResult = new JSONObject();
    noopEnrichmentResult.put(adapterPerfEndKey, "" + System.currentTimeMillis());
    return CompletableFuture.completedFuture(Either.Data(noopEnrichmentResult));
  }


  /**
   * Take a message and a config and return a list of tasks indexed by adapter enrichment types.
   * @param message
   * @param enrichmentStrategy
   * @param config
   * @return
   */
  public Map<String, List<JSONObject>> splitMessage( JSONObject message
                                                   , EnrichmentStrategy enrichmentStrategy
                                                   , SensorEnrichmentConfig config
                                                   ) {
    Map<String, List<JSONObject>> streamMessageMap = new HashMap<>();
    Map<String, Object> enrichmentFieldMap = enrichmentStrategy.getUnderlyingConfig(config).getFieldMap();

    Map<String, ConfigHandler> fieldToHandler = enrichmentStrategy.getUnderlyingConfig(config).getEnrichmentConfigs();

    Set<String> enrichmentTypes = new HashSet<>(enrichmentFieldMap.keySet());

    //the set of enrichments configured
    enrichmentTypes.addAll(fieldToHandler.keySet());

    //For each of these enrichment types, we're going to construct JSONObjects
    //which represent the individual enrichment tasks.
    for (String enrichmentType : enrichmentTypes) {
      Object fields = enrichmentFieldMap.get(enrichmentType);
      ConfigHandler retriever = fieldToHandler.get(enrichmentType);

      //How this is split depends on the ConfigHandler
      List<JSONObject> enrichmentObject = retriever.getType()
              .splitByFields( message
                      , fields
                      , field -> enrichmentStrategy.fieldToEnrichmentKey(enrichmentType, field)
                      , retriever
              );
      streamMessageMap.put(enrichmentType, enrichmentObject);
    }
    return streamMessageMap;
  }

}
