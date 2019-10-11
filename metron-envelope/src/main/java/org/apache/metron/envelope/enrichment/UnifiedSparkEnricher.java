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

import org.apache.metron.common.configuration.EnrichmentConfigurations;
import org.apache.metron.common.configuration.enrichment.SensorEnrichmentConfig;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.performance.PerformanceLogger;
import org.apache.metron.common.utils.MessageUtils;
import org.apache.metron.enrichment.adapters.maxmind.asn.GeoLiteAsnDatabase;
import org.apache.metron.enrichment.adapters.maxmind.geo.GeoLiteCityDatabase;
import org.apache.metron.enrichment.cache.CacheKey;
import org.apache.metron.enrichment.configuration.Enrichment;
import org.apache.metron.enrichment.interfaces.EnrichmentAdapter;
import org.apache.metron.enrichment.parallel.ConcurrencyContext;
import org.apache.metron.enrichment.parallel.EnrichmentContext;
import org.apache.metron.enrichment.parallel.EnrichmentStrategies;
import org.apache.metron.enrichment.parallel.WorkerPoolStrategies;
import org.apache.metron.envelope.ZookeeperClient;
import org.apache.metron.envelope.config.EnrichmentConfigManager;
import org.apache.metron.envelope.encoding.SparkRowEncodingStrategy;
import org.apache.metron.envelope.utils.Either;
import org.apache.metron.envelope.utils.MetronErrorProcessor;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.spark.api.java.Optional;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static org.apache.metron.common.Constants.STELLAR_CONTEXT_CONF;

/**
 * A spark port of the unified enrichment/threat intel bolt.  In contrast to the split/enrich/join
 * bolts above, this handles the entire enrichment lifecycle in one bolt using a threadpool to
 * enrich in parallel.
 *
 * From an architectural perspective, this is a divergence from the polymorphism based strategy we have
 * used in the split/join bolts.  Rather, this bolt is provided a strategy to use, either enrichment or threat intel,
 * through composition.  This allows us to move most of the implementation into components independent
 * from Storm.  This will greater facilitate reuse.
 */
public class UnifiedSparkEnricher implements AutoCloseable {
  private final SparkRowEncodingStrategy encodingStrategy;
  private MetronErrorProcessor metronErrorProcessor;
  public static class Perf {} // used for performance logging
  private PerformanceLogger perfLog; // not static bc multiple bolts may exist in same worker

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * The number of threads in the threadpool.  One threadpool is created per process.
   * This is a topology-level configuration
   */
  public static final String THREADPOOL_NUM_THREADS_TOPOLOGY_CONF = "metron.threadpool.size";
  /**
   * The type of threadpool to create. This is a topology-level configuration.
   */
  public static final String THREADPOOL_TYPE_TOPOLOGY_CONF = "metron.threadpool.type";

  private static volatile EnrichmentConfigManager enrichmentConfigManager;

  /**
   * The enricher implementation to use.  This will do the parallel enrichment via a thread pool.
   */
  private SparkParallelEnricher enricher;

  /**
   * The strategy to use for this enrichment bolt.  Practically speaking this is either
   * enrichment or threat intel.  It is configured in the topology itself.
   */
  private EnrichmentStrategies strategy;
  /**
   * Determine the way to retrieve the message.  This must be specified in the topology.
   */

  private static volatile Context stellarContext;
  /**
   * An enrichment type to adapter map.  This is configured externally.
   */
  private Map<String, EnrichmentAdapter<CacheKey>> enrichmentsByType = new HashMap<>();

  /**
   * The total number of elements in a LRU cache.  This cache is used for the enrichments; if an
   * element is in the cache, then the result is returned instead of computed.
   */
  private Long maxCacheSize;
  /**
   * The total amount of time in minutes since write to keep an element in the cache.
   */
  private Long maxTimeRetain;
  /**
   * If the bolt is reloaded, invalidate the cache?
   */
  private boolean invalidateCacheOnReload = false;

  private EnrichmentContext enrichmentContext;
  private boolean captureCacheStats = true;

  private static synchronized void initStellarContext(String zookeeperQuorum, EnrichmentConfigManager enrichmentConfigManager) {
    Context tmpStellarContext = new Context.Builder()
            .with(Context.Capabilities.ZOOKEEPER_CLIENT, () -> ZookeeperClient.getZKInstance(zookeeperQuorum))
            .with(Context.Capabilities.GLOBAL_CONFIG, () -> enrichmentConfigManager.getConfigurations().getGlobalConfig())
            .with(Context.Capabilities.STELLAR_CONFIG,() -> enrichmentConfigManager.getConfigurations().getGlobalConfig())
            .build();
    StellarFunctions.initialize(tmpStellarContext);
    stellarContext = tmpStellarContext;
  }

  private static synchronized void initEnrichmentConfigManager(String zookeeperQuorum) {
    EnrichmentConfigManager tmpEnrichmentConfigManager = new EnrichmentConfigManager(
            Objects.requireNonNull(zookeeperQuorum)
    );
    tmpEnrichmentConfigManager.init();
    enrichmentConfigManager = tmpEnrichmentConfigManager;
  }

  public UnifiedSparkEnricher(@NotNull String zookeeperQuorum, @NotNull String kafkaBrokers,
                              @NotNull SparkRowEncodingStrategy rowEncodingStrategy,
                              @NotNull Map<String, EnrichmentAdapter<CacheKey>> enrichmentsByType,
                              Map<String, String> additionalConfig) {
    this.encodingStrategy = Objects.requireNonNull(rowEncodingStrategy);
    if (enrichmentConfigManager == null) {
      initEnrichmentConfigManager(zookeeperQuorum);
    }
    if (stellarContext == null) {
      initStellarContext(zookeeperQuorum, enrichmentConfigManager);
    }
    metronErrorProcessor = new MetronErrorProcessor(kafkaBrokers);

    if (this.maxCacheSize == null) {
      throw new IllegalStateException("MAX_CACHE_SIZE_OBJECTS_NUM must be specified");
    }
    if (this.maxTimeRetain == null) {
      throw new IllegalStateException("MAX_TIME_RETAIN_MINUTES must be specified");
    }
    if (this.enrichmentsByType.isEmpty()) {
      throw new IllegalStateException("Adapter must be specified");
    }

    for(Entry<String, EnrichmentAdapter<CacheKey>> adapterKv : enrichmentsByType.entrySet()) {
      boolean success = adapterKv.getValue().initializeAdapter(getConfigurations().getGlobalConfig());
      if (!success) {
        LOG.error("[Metron] Could not initialize adapter: " + adapterKv.getKey());
        throw new IllegalStateException("Could not initialize adapter: " + adapterKv.getKey());
      }
    }
    WorkerPoolStrategies workerPoolStrategy = WorkerPoolStrategies.FIXED;
    if(additionalConfig.containsKey(THREADPOOL_TYPE_TOPOLOGY_CONF)) {
      workerPoolStrategy = WorkerPoolStrategies.valueOf(additionalConfig.get(THREADPOOL_TYPE_TOPOLOGY_CONF) + "");
    }

    enricher = new SparkParallelEnricher(enrichmentsByType,
            captureCacheStats,
            maxCacheSize,
            maxTimeRetain,
            invalidateCacheOnReload,
            getNumThreads(additionalConfig.get(THREADPOOL_NUM_THREADS_TOPOLOGY_CONF)),
            workerPoolStrategy
    );

    perfLog = new PerformanceLogger(() -> getConfigurations().getGlobalConfig(), Perf.class.getName());
    GeoLiteCityDatabase.INSTANCE.update((String)getConfigurations().getGlobalConfig().get(
            GeoLiteCityDatabase.GEO_HDFS_FILE));
    GeoLiteAsnDatabase.INSTANCE.update((String)getConfigurations().getGlobalConfig().get(
            GeoLiteAsnDatabase.ASN_HDFS_FILE));
    enrichmentContext = new EnrichmentContext(StellarFunctions.FUNCTION_RESOLVER(), stellarContext);

    // Make sure Stallar is available for all sourceTypes
    getConfigurations().getTypes().forEach(sourceType -> {
      SensorEnrichmentConfig config = getConfigurations().getSensorEnrichmentConfig(sourceType);
      config.getConfiguration().putIfAbsent(STELLAR_CONTEXT_CONF, stellarContext);
    });
  }

  private EnrichmentConfigurations getConfigurations() {
    return enrichmentConfigManager.getConfigurations();
  }

  /**
   * Specify the enrichments to support.
   * @param enrichments enrichment
   * @return Instance of this class
   */
  public UnifiedSparkEnricher withEnrichments(List<Enrichment> enrichments) {
    for(Enrichment e : enrichments) {
      enrichmentsByType.put(e.getType(), e.getAdapter());
    }
    return this;
  }

  public UnifiedSparkEnricher withCaptureCacheStats(boolean captureCacheStats) {
    this.captureCacheStats = captureCacheStats;
    return this;
  }

  /**
   * Figure out how many threads to use in the thread pool.  The user can pass an arbitrary object, so parse it
   * according to some rules.  If it's a number, then cast to an int.  IF it's a string and ends with "C", then strip
   * the C and treat it as an integral multiple of the number of cores.  If it's a string and does not end with a C, then treat
   * it as a number in string form.  If its null then just return 2 * Num available processors
   * @param numThreads
   * @return
   */
  private static int getNumThreads(Object numThreads) {
    if(numThreads instanceof Number) {
      return ((Number)numThreads).intValue();
    }
    else if(numThreads instanceof String) {
      String numThreadsStr = ((String)numThreads).trim().toUpperCase();
      if(numThreadsStr.endsWith("C")) {
        int factor = Integer.parseInt(numThreadsStr.replace("C", ""));
        return factor*Runtime.getRuntime().availableProcessors();
      }
      else {
        return Integer.parseInt(numThreadsStr);
      }
    }
    return 2*Runtime.getRuntime().availableProcessors();
  }

  /**
   * The strategy to use.  This indicates which part of the config that this bolt uses
   * to enrich, threat intel or enrichment.  This must conform to one of the EnrichmentStrategies
   * enum.
   * @param strategy
   * @return
   */
  public UnifiedSparkEnricher withStrategy(String strategy) {
    this.strategy = EnrichmentStrategies.valueOf(strategy);
    return this;
  }

  /**
   * @param maxCacheSize Maximum size of cache before flushing
   * @return Instance of this class
   */
  public UnifiedSparkEnricher withMaxCacheSize(long maxCacheSize) {
    this.maxCacheSize = maxCacheSize;
    return this;
  }

  /**
   * @param maxTimeRetain Maximum time to retain cached entry before expiring
   * @return Instance of this class
   */

  public UnifiedSparkEnricher withMaxTimeRetain(long maxTimeRetain) {
    this.maxTimeRetain = maxTimeRetain;
    return this;
  }

  /**
   * Invalidate the cache on reload of configuration.  By default, we do not.
   * @param cacheInvalidationOnReload
   * @return
   */
  public UnifiedSparkEnricher withCacheInvalidationOnReload(boolean cacheInvalidationOnReload) {
    this.invalidateCacheOnReload= cacheInvalidationOnReload;
    return this;
  }

  public List<CompletableFuture<Either<MetronError,JSONObject>>> asyncEnrich(JSONObject message) {
    final String sourceType = MessageUtils.getSensorType(message);
    SensorEnrichmentConfig config = getConfigurations().getSensorEnrichmentConfig(sourceType);
    if(config == null) {
      LOG.warn("Unable to find SensorEnrichmentConfig for sourceType: {}", sourceType);
      config = new SensorEnrichmentConfig();
    }
    return enricher.apply(message, strategy, config, perfLog);
  }

  @Override
  public void close() throws Exception {
    metronErrorProcessor.close();
  }
}
