package org.apache.metron.envelope.enrichment;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.curator.framework.CuratorFramework;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.ConfigurationsUtils;
import org.apache.metron.common.configuration.EnrichmentConfigurations;
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
import org.apache.metron.stellar.common.utils.JSONUtils;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.spark.sql.Row;
import org.json.simple.JSONObject;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.metron.common.Constants.STELLAR_CONTEXT_CONF;
import static org.apache.metron.stellar.common.configuration.ConfigurationsUtils.readGlobalConfigBytesFromZookeeper;

public class MetronSparkPartitionEnricher implements envelope.shaded.com.google.common.base.Function<Row, Iterable<Row>>{
  public static class Perf {} // used for performance logging
  private static LazyLogger LOG = LazyLoggerFactory.getLogger(MetronSparkPartitionEnricher.class);
  private PerformanceLogger perfLog; // not static bc multiple bolts may exist in same worker
  private String zookeeperQuorum;
  private SparkRowEncodingStrategy encodingStrategy;
  private Map<String, Object> globalConfig;
  private EnrichmentConfigManager enrichmentConfigManager;

  private Context stellarContext;
  protected String enrichmentType;
  protected EnrichmentAdapter<CacheKey> adapter;
  protected transient CacheLoader<CacheKey, JSONObject> loader;
  protected transient LoadingCache<CacheKey, JSONObject> cache;
  protected Long maxCacheSize;
  protected Long maxTimeRetain;
  protected boolean invalidateCacheOnReload = false;

  /**
   * This object gets reconstructed for every partition of data,
   * so it re-reads its configuration fresh from zookeeper
   * @param zookeeperQuorum Zookeeper address of configuration
   * @throws Exception if zookeeper error occurs
   */
  public MetronSparkPartitionEnricher(String zookeeperQuorum, SparkRowEncodingStrategy rowEncodingStrategy) {
    this.zookeeperQuorum = zookeeperQuorum;
    this.encodingStrategy = rowEncodingStrategy;
    this.enrichmentConfigManager = new EnrichmentConfigManager(zookeeperQuorum);
  }

  /**
   * Prepares the object for processing
   */
  public void init() throws Exception {
    this.encodingStrategy.init();
    CuratorFramework curatorFramework = ZookeeperClient.getZKInstance(zookeeperQuorum);

    enrichmentConfigManager.init();
    if (this.maxCacheSize == null)
      throw new IllegalStateException("MAX_CACHE_SIZE_OBJECTS_NUM must be specified");
    if (this.maxTimeRetain == null)
      throw new IllegalStateException("MAX_TIME_RETAIN_MINUTES must be specified");
    if (this.adapter == null)
      throw new IllegalStateException("Adapter must be specified");
    loader = key -> adapter.enrich(key);
    cache = Caffeine.newBuilder().maximumSize(maxCacheSize)
            .expireAfterWrite(maxTimeRetain, TimeUnit.MINUTES)
            .build(loader);
    boolean success = adapter.initializeAdapter(enrichmentConfigManager.getConfigurations().getGlobalConfig());
    if (!success) {
      LOG.error("[Metron] MetronSparkPartitionEnricher could not initialize enrichment adapter");
      throw new IllegalStateException("Could not initialize enrichment adapter...");
    }
    perfLog = new PerformanceLogger(() -> enrichmentConfigManager.getConfigurations().getGlobalConfig(),
            MetronSparkPartitionEnricher.Perf.class.getName());
    initializeStellar();
  }

  /**
   * @param enrichment enrichment
   * @return Instance of this class
   */
  public MetronSparkPartitionEnricher withEnrichment(Enrichment enrichment) {
    this.enrichmentType = enrichment.getType();
    this.adapter = enrichment.getAdapter();
    return this;
  }

  /**
   * @param maxCacheSize Maximum size of cache before flushing
   * @return Instance of this class
   */
  public MetronSparkPartitionEnricher withMaxCacheSize(long maxCacheSize) {
    this.maxCacheSize = maxCacheSize;
    return this;
  }

  /**
   * @param maxTimeRetain Maximum time to retain cached entry before expiring
   * @return Instance of this class
   */
  public MetronSparkPartitionEnricher withMaxTimeRetain(long maxTimeRetain) {
    this.maxTimeRetain = maxTimeRetain;
    return this;
  }

  public MetronSparkPartitionEnricher withCacheInvalidationOnReload(boolean cacheInvalidationOnReload) {
    this.invalidateCacheOnReload= cacheInvalidationOnReload;
    return this;
  }

  protected void initializeStellar() {
    stellarContext = new Context.Builder()
            .with(Context.Capabilities.ZOOKEEPER_CLIENT, () -> ZookeeperClient.getZKInstance(zookeeperQuorum))
            .with(Context.Capabilities.GLOBAL_CONFIG, () -> enrichmentConfigManager.getConfigurations().getGlobalConfig())
            .with(Context.Capabilities.STELLAR_CONFIG,() -> enrichmentConfigManager.getConfigurations().getGlobalConfig())
            .build();
    StellarFunctions.initialize(stellarContext);
  }

  /**
   * Run the configured list of Metron Enrichers across the passed Row
   * If any errors are present, pass them back encoded into the return row values
   * Input rows are expected to
   * Output spark sql rows of processed data (success and errors) with schema 'outputSchema'
   */
  @Nullable
  @Override
  public Iterable<Row> apply(@Nullable Row row) {
    perfLog.mark("execute");
    String key = tuple.getStringByField("key");
    JSONObject rawMessage = (JSONObject) tuple.getValueByField("message");
    String subGroup = "";

    JSONObject enrichedMessage = new JSONObject();
    enrichedMessage.put("adapter." + adapter.getClass().getSimpleName().toLowerCase() + ".begin.ts", "" + System.currentTimeMillis());
    try {
      if (rawMessage == null || rawMessage.isEmpty())
        throw new Exception("Could not parse binary stream to JSON");
      if (key == null)
        throw new Exception("Key is not valid");
      String sourceType = null;
      if(rawMessage.containsKey(Constants.SENSOR_TYPE)) {
        sourceType = rawMessage.get(Constants.SENSOR_TYPE).toString();
      }
      else {
        throw new RuntimeException("Source type is missing from enrichment fragment: " + rawMessage.toJSONString());
      }
      String prefix = null;
      for (Object o : rawMessage.keySet()) {
        String field = (String) o;
        Object value =  rawMessage.get(field);
        if (field.equals(Constants.SENSOR_TYPE)) {
          enrichedMessage.put(Constants.SENSOR_TYPE, value);
        } else {
          JSONObject enrichedField = new JSONObject();
          if (value != null) {
            SensorEnrichmentConfig config = () -> enrichmentConfigManager.getConfigurations().getSensorEnrichmentConfig(sourceType);
            if(config == null) {
              LOG.debug("Unable to find SensorEnrichmentConfig for sourceType: {}", sourceType);
              MetronError metronError = new MetronError()
                      .withErrorType(Constants.ErrorType.ENRICHMENT_ERROR)
                      .withMessage("Unable to find SensorEnrichmentConfig for sourceType: " + sourceType)
                      .addRawMessage(rawMessage);
              StormErrorUtils.handleError(collector, metronError);
              continue;
            }
            config.getConfiguration().putIfAbsent(STELLAR_CONTEXT_CONF, stellarContext);
            CacheKey cacheKey= new CacheKey(field, value, config);
            try {
              adapter.logAccess(cacheKey);
              prefix = adapter.getOutputPrefix(cacheKey);
              subGroup = adapter.getStreamSubGroup(enrichmentType, field);

              perfLog.mark("enrich");
              enrichedField = cache.get(cacheKey);
              perfLog.log("enrich", "key={}, time to run enrichment type={}", key, enrichmentType);

              if (enrichedField == null)
                throw new Exception("[Metron] Could not enrich string: "
                        + value);
            }
            catch(Exception e) {
              LOG.error(e.getMessage(), e);
              MetronError metronError = new MetronError()
                      .withErrorType(Constants.ErrorType.ENRICHMENT_ERROR)
                      .withThrowable(e)
                      .withErrorFields(new HashSet() {{ add(field); }})
                      .addRawMessage(rawMessage);
              StormErrorUtils.handleError(collector, metronError);
              continue;
            }
          }
          enrichedMessage = EnrichmentUtils.adjustKeys(enrichedMessage, enrichedField, field, prefix);
        }
      }

      enrichedMessage.put("adapter." + adapter.getClass().getSimpleName().toLowerCase() + ".end.ts", "" + System.currentTimeMillis());
      if (!enrichedMessage.isEmpty()) {
        collector.emit(enrichmentType, new Values(key, enrichedMessage, subGroup));
      }
    } catch (Exception e) {
      handleError(key, rawMessage, subGroup, enrichedMessage, e);
    }
    perfLog.log("execute", "key={}, elapsed time to run execute", key);
  }
}
