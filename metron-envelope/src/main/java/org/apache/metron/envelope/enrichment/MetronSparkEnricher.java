package org.apache.metron.envelope.enrichment;

import com.cloudera.labs.envelope.spark.RowWithSchema;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import envelope.shaded.com.google.common.base.Preconditions;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.enrichment.SensorEnrichmentConfig;
import org.apache.metron.common.error.MetronError;
import org.apache.metron.common.performance.PerformanceLogger;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.metron.enrichment.cache.CacheKey;
import org.apache.metron.enrichment.configuration.Enrichment;
import org.apache.metron.enrichment.interfaces.EnrichmentAdapter;
import org.apache.metron.enrichment.utils.EnrichmentUtils;
import org.apache.metron.envelope.ZookeeperClient;
import org.apache.metron.envelope.config.EnrichmentConfigManager;
import org.apache.metron.envelope.encoding.SparkRowEncodingStrategy;
import org.apache.metron.envelope.utils.Either;
import org.apache.metron.envelope.utils.MetronErrorProcessor;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.metron.common.Constants.STELLAR_CONTEXT_CONF;
import static org.apache.metron.envelope.utils.ErrorUtils.convertErrorsToMetronErrors;
import static org.apache.metron.envelope.utils.ErrorUtils.convertSparkErrorsToMetronErrors;

public class MetronSparkEnricher implements envelope.shaded.com.google.common.base.Function<Row, Iterable<Row>>,
AutoCloseable {
  public static class Perf {} // used for performance logging
  private static LazyLogger LOG = LazyLoggerFactory.getLogger(MetronSparkEnricher.class);
  private static volatile Context stellarContext;
  private static volatile EnrichmentConfigManager enrichmentConfigManager;

  private PerformanceLogger perfLog; // not static bc multiple bolts may exist in same worker
  @NotNull private String zookeeperQuorum;
  @NotNull private SparkRowEncodingStrategy encodingStrategy;
  @NotNull private String kafkaBrokers;
  @NotNull private static ConcurrentHashMap<String, LoadingCache<CacheKey, JSONObject>> cacheMap = new ConcurrentHashMap<>();
  private MetronErrorProcessor metronErrorProcessor;

  private String enrichmentType;
  private EnrichmentAdapter<CacheKey> adapter;
  private transient LoadingCache<CacheKey, JSONObject> cache;
  private Long maxCacheSize;
  private Long maxTimeRetain;
  private boolean invalidateCacheOnReload = false;

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

  /**
   * This object gets reconstructed for every partition of data,
   * so it re-reads its configuration fresh from zookeeper
   * @param zookeeperQuorum Zookeeper address of configuration
   * @param rowEncodingStrategy row encoder to use
   * @param kafkaBrokers Handles any processing errors
   */
  MetronSparkEnricher(@NotNull String zookeeperQuorum,
                      @NotNull String kafkaBrokers,
                      @NotNull SparkRowEncodingStrategy rowEncodingStrategy) {
    this.zookeeperQuorum = Objects.requireNonNull(zookeeperQuorum);
    this.encodingStrategy = Objects.requireNonNull(rowEncodingStrategy);
    this.kafkaBrokers = Objects.requireNonNull(kafkaBrokers);

    if (enrichmentConfigManager == null) {
      initEnrichmentConfigManager(zookeeperQuorum);
    }

    metronErrorProcessor = new MetronErrorProcessor(kafkaBrokers, null);

    if (this.maxCacheSize == null)
      throw new IllegalStateException("MAX_CACHE_SIZE_OBJECTS_NUM must be specified");
    if (this.maxTimeRetain == null)
      throw new IllegalStateException("MAX_TIME_RETAIN_MINUTES must be specified");
    if (this.adapter == null)
      throw new IllegalStateException("Adapter must be specified");

    final Supplier<LoadingCache<CacheKey, JSONObject>> cacheSupplier = () -> Caffeine.newBuilder()
            .maximumSize(maxCacheSize)
            .expireAfterWrite(maxTimeRetain, TimeUnit.MINUTES)
            .build(key -> adapter.enrich(key));

    // Make this cache global for this type of enrichment, so that it will cover enrichment
    // operations over multiple spark partitions
    if (invalidateCacheOnReload) {
      // Force creation of a new cache
      cache = cacheMap.compute(enrichmentType, (k, existingCache) -> {
        if (existingCache != null) existingCache.cleanUp();
        return cacheSupplier.get();
      });
    } else {
      // Re-use existing cache if present, otherwise create
      cache = cacheMap.computeIfAbsent(enrichmentType, k -> cacheSupplier.get());
    }

    boolean success = adapter.initializeAdapter(enrichmentConfigManager.getConfigurations().getGlobalConfig());
    if (!success) {
      LOG.error("[Metron] MetronSparkPartitionEnricher could not initialize enrichment adapter");
      throw new IllegalStateException("Could not initialize enrichment adapter...");
    }
    perfLog = new PerformanceLogger(() -> enrichmentConfigManager.getConfigurations().getGlobalConfig(),
            MetronSparkEnricher.Perf.class.getName());

    if (stellarContext == null) {
      initStellarContext(zookeeperQuorum, enrichmentConfigManager);
    }
  }


  @Override
  public void close() throws Exception {
    metronErrorProcessor.close();
  }

  /**
   * @param enrichment enrichment
   * @return Instance of this class
   */
  public MetronSparkEnricher withEnrichment(Enrichment enrichment) {
    this.enrichmentType = enrichment.getType();
    this.adapter = enrichment.getAdapter();
    return this;
  }

  /**
   * @param maxCacheSize Maximum size of cache before flushing
   * @return Instance of this class
   */
  public MetronSparkEnricher withMaxCacheSize(long maxCacheSize) {
    this.maxCacheSize = maxCacheSize;
    return this;
  }

  /**
   * @param maxTimeRetain Maximum time to retain cached entry before expiring
   * @return Instance of this class
   */
  public MetronSparkEnricher withMaxTimeRetain(long maxTimeRetain) {
    this.maxTimeRetain = maxTimeRetain;
    return this;
  }

  public MetronSparkEnricher withCacheInvalidationOnReload(boolean cacheInvalidationOnReload) {
    this.invalidateCacheOnReload= cacheInvalidationOnReload;
    return this;
  }

  /**
   * Run the configured list of Metron Enrichers across the passed Row
   * If any errors are present, pass them back encoded into the return row values
   * Input rows are expected to
   * Output spark sql rows of processed data (success and errors) with schema 'outputSchema'
   */
  @NotNull
  @Override
  public Iterable<Row> apply(@Nullable Row nullableRow) {
    if (nullableRow == null) {
      LOG.warn("Passed a null row - skipping");
      return Collections.emptyList();
    }
    @NotNull final Row row = Objects.requireNonNull(nullableRow);

    // Deserialise message from spark row
    Stream<Either<MetronError, JSONObject>> deSerialisedMessages = Stream.of(row)
            .map(convertSparkErrorsToMetronErrors(Constants.ErrorType.ENRICHMENT_ERROR,
                    x -> encodingStrategy.decodeParsedMessageFromKafka(x)));

    // enrich message with configured enrichment
    Stream<Either<MetronError, JSONObject>> enrichedMessages = deSerialisedMessages
            .flatMap(Either::getDataStream) // filters out decode errors and converts to JSONObjects
            .flatMap(this::enrichMessage);

    // Serialise message back into spark row
    Stream<Either<MetronError, RowWithSchema>> serialisedMessages = enrichedMessages
            .flatMap(Either::getDataStream) // filters out enrich errors and converts to RowWithSchema
            .map(convertErrorsToMetronErrors(Constants.ErrorType.ENRICHMENT_ERROR,
                    x -> encodingStrategy.encodeEnrichedMessageIntoSparkRow(x)));

    // handle any errors that occurred during the previous 3 steps
    Stream.of(
            deSerialisedMessages.flatMap(Either::getErrorStream), // filters for and extracts MetronErrors
            enrichedMessages.flatMap(Either::getErrorStream), // filters for and extracts MetronErrors
            serialisedMessages.flatMap(Either::getErrorStream) // filters for and extracts MetronErrors
            )
            .flatMap(Function.identity()) // flatten to a simple stream of MetronErrors
            .forEach( err -> {
              LOG.warn("Error occurred during enriching: {}", err.getJSONObject().toJSONString());
              metronErrorProcessor.handleMetronError(err);
            });

    // return all successfully enriched, serialised messages back to caller
    return serialisedMessages
            .flatMap(Either::getDataStream)  // filters for and extracts Spark Rows
            .collect(Collectors.toList());
  }

  /**
   * Enrich a single Metron Message
   * @param rawMessage Message to enrich
   * @return A stream of errors (if any), plus one enriched JSONObject
   */
   private Stream<Either<MetronError,JSONObject>> enrichMessage(@NotNull JSONObject rawMessage) {
    List<MetronError> errors = new ArrayList<>();
    perfLog.mark("execute");
    // random key right now just for tracking enrichment performance
    String key = UUID.randomUUID().toString();
    JSONObject enrichedMessage = new JSONObject();
    enrichedMessage.put("adapter." + adapter.getClass().getSimpleName().toLowerCase() + ".begin.ts", "" + System.currentTimeMillis());
    try {
      Preconditions.checkState(!rawMessage.isEmpty(), "Could not parse binary stream to JSON");
      Preconditions.checkState(rawMessage.containsKey(Constants.SENSOR_TYPE),
              "Source type is missing from enrichment fragment: " + rawMessage.toJSONString() );
      final String sourceType = rawMessage.get(Constants.SENSOR_TYPE).toString();
      final SensorEnrichmentConfig config = enrichmentConfigManager.getConfigurations().getSensorEnrichmentConfig(sourceType);

      String prefix = null;
      for (Object o : rawMessage.keySet()) {
        String field = (String) o;
        Object value =  rawMessage.get(field);
        if (field.equals(Constants.SENSOR_TYPE)) {
          enrichedMessage.put(Constants.SENSOR_TYPE, value);
        } else {
          JSONObject enrichedField = new JSONObject();
          if (value != null) {
            if(config == null) {
              LOG.debug("Unable to find SensorEnrichmentConfig for sourceType: {}", sourceType);
              errors.add(new MetronError()
                      .withErrorType(Constants.ErrorType.ENRICHMENT_ERROR)
                      .withMessage("Unable to find SensorEnrichmentConfig for sourceType: " + sourceType)
                      .addRawMessage(rawMessage));
              continue;
            }
            config.getConfiguration().putIfAbsent(STELLAR_CONTEXT_CONF, stellarContext);
            CacheKey cacheKey= new CacheKey(field, value, config);
            try {
              adapter.logAccess(cacheKey);
              prefix = adapter.getOutputPrefix(cacheKey);
              perfLog.mark("enrich");
              enrichedField = cache.get(cacheKey);
              perfLog.log("enrich", "key={}, time to run enrichment type={}", key, enrichmentType);

              if (enrichedField == null)
                throw new Exception("[Metron] Could not enrich string: "
                        + value);
            }
            catch(Exception e) {
              LOG.error(e.getMessage(), e);
              errors.add(new MetronError()
                      .withErrorType(Constants.ErrorType.ENRICHMENT_ERROR)
                      .withThrowable(e)
                      .withErrorFields(Collections.singleton(field))
                      .addRawMessage(rawMessage));
              continue;
            }
          }
          enrichedMessage = EnrichmentUtils.adjustKeys(enrichedMessage, enrichedField, field, prefix);
        }
      }

      enrichedMessage.put("adapter." + adapter.getClass().getSimpleName().toLowerCase() + ".end.ts", "" + System.currentTimeMillis());

    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      errors.add(new MetronError()
              .withErrorType(Constants.ErrorType.ENRICHMENT_ERROR)
              .withThrowable(e)
              .addRawMessage(rawMessage));
    }
    perfLog.log("execute", "key={}, elapsed time to run execute", key);

    return Stream.concat(
            Stream.of(enrichedMessage).map(Either::Data),  // Wrap enriched message in an 'Either' Struct
            errors.stream().map(Either::Error) // Wrap MetronErrors in 'Either' Structs
    );
  }

}
