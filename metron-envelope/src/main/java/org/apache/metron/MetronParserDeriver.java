package org.apache.metron;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.derive.Deriver;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import envelope.shaded.com.google.common.collect.FluentIterable;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import parquet.Preconditions;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * Adapts metron parsers to the Envelope Deriver abstraction
 */
public class MetronParserDeriver implements Deriver, ProvidesAlias {
  private static final String ALIAS = "MetronParser";
  private static final String ZOOKEEPER = "zookeeper";
  private String zookeeperQuorum;

  @Override
  public String getAlias() {
    return ALIAS;
  }

  // envelope configuration
  @Override
  public void configure(Config config) {
    zookeeperQuorum = Objects.requireNonNull(config.getString(ZOOKEEPER), "Zookeeper quorum is required");
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> map)  {
    Preconditions.checkArgument(map.size() == 1, getAlias() + " should only have one dependant dataset");
    final Dataset<Row> src = Iterables.getOnlyElement(map.entrySet()).getValue();
    final Dataset<Row> dst = src.mapPartitions(new MapPartitionsFunction<Row, Row>() {
      // The following function gets created from scratch for every spark partition processed
      @Override
      public Iterator<Row> call(Iterator<Row> iterator) throws Exception {
        final CBORFactory f = new CBORFactory();
        final ObjectMapper mapper = new ObjectMapper(f);
        final MetronSparkPartitionParser metronSparkPartitionParser = new MetronSparkPartitionParser(mapper, zookeeperQuorum);
        // we can get away with this as long as the iterator is only used once;
        // we use iterator transforms so we can stream iterator processing
        // so we do not require the entire dataset to be pulled into memory at once
        return FluentIterable.from(() -> iterator)
                // transformAndConcat is the guava equivalent of a flatmap
                .transformAndConcat(metronSparkPartitionParser)
                .iterator();
      }
    }, RowEncoder.apply(MetronSparkPartitionParser.getOutputSchema()));

    return dst;
  }

}
