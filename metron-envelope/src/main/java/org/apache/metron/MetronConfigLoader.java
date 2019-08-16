package org.apache.metron;

import com.cloudera.labs.envelope.configuration.ConfigLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.kudu.shaded.com.google.common.base.MoreObjects;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class MetronConfigLoader implements ConfigLoader {
  private static final String DEFAULT_ENVELOPE_CONFIG_PATH = "envelope.config";
  private static final String ZOOKEEPER = "Zookeeper";
  private static final String ZOOKEEPER_NODE_NAME = "ZookeeperNodeName";

  private static Logger LOG = LoggerFactory.getLogger(MetronConfigLoader.class);

  private String zookeeperQuorum;
  private String zookeeperNodeName;

  private volatile Config metronConfig;
  private synchronized Config getMetronConfig() {
    return metronConfig;
  }
  private synchronized void setMetronConfig(Config config) {
    this.metronConfig = config;
  }

  /**
   * Needs to return a Metron config object
   * @return
   */
  @Override
  public Config getConfig() {
    Objects.requireNonNull(zookeeperQuorum);
    try {
      if (getMetronConfig() == null) {
        synchronized (MetronConfigLoader.class) {
          if (getMetronConfig() == null) {
            final CuratorFramework zkClient = ZookeeperClient.getZKInstance(zookeeperQuorum);
            setMetronConfig(readMetronConfigFromZK(zkClient));
            setDataWatch(zkClient);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Error reading configuration form zookeeper", e);
    }
    return getMetronConfig();
  }

  private Config readMetronConfigFromZK(final CuratorFramework zkClient) throws Exception {
    final byte[] configBytes = zkClient.getData().forPath(zookeeperNodeName);
    return extractConfig(configBytes);
  }

  private void setDataWatch(final CuratorFramework zkClient) {
    try {
      zkClient.checkExists()
              .usingWatcher((CuratorWatcher) watchedEvent -> {
                if (watchedEvent.getType() == Watcher.Event.EventType.NodeDataChanged) {
                  LOG.info("Change of configuration detected");
                  setMetronConfig(readMetronConfigFromZK(zkClient));
                }
                // a watch only throws once, then it needs to be reset.
                setDataWatch(zkClient);
              })
              .forPath(zookeeperNodeName);
    } catch (Exception e) {
      LOG.error("Error setting data watch on Metron config node", e);
    }
  }

  private Config extractConfig(byte[] updatedBytes) {
    return ConfigFactory.parseString(new String(updatedBytes, StandardCharsets.UTF_8), ConfigParseOptions.defaults());
  }

  @Override
  public void configure(Config config) {
    zookeeperQuorum = Objects.requireNonNull(config.getString(ZOOKEEPER), "Zookeeper Quorum required");
    zookeeperNodeName = MoreObjects.firstNonNull(config.getString(ZOOKEEPER_NODE_NAME), DEFAULT_ENVELOPE_CONFIG_PATH);
  }
}
