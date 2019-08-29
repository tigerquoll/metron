/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.metron.envelope;

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

  /**
   * Gets or creates a new metron config
   * @return Metron configuration object
   */
  @Override
  public synchronized Config getConfig() {
    Objects.requireNonNull(zookeeperQuorum);
    try {
      if (metronConfig == null) {
        final CuratorFramework zkClient = ZookeeperClient.getZKInstance(zookeeperQuorum);
        metronConfig = readMetronConfigFromZK(zkClient);
        setDataWatch(zkClient);
      }
    } catch (Exception e) {
      LOG.error("Error reading configuration form zookeeper", e);
    }
    return metronConfig;
  }

  private synchronized void setConfig(Config config) {
    this.metronConfig = config;
  }

  private Config readMetronConfigFromZK(final CuratorFramework zkClient) throws Exception {
    final byte[] configBytes = zkClient.getData().forPath(zookeeperNodeName);
    return ConfigFactory.parseString(new String(configBytes, StandardCharsets.UTF_8), ConfigParseOptions.defaults());
  }

  private void setDataWatch(final CuratorFramework zkClient) {
    try {
      zkClient.checkExists()
              .usingWatcher((CuratorWatcher) watchedEvent -> {
                if (watchedEvent.getType() == Watcher.Event.EventType.NodeDataChanged) {
                  LOG.info("Change of configuration detected - forcing a reload next time config needed");
                  setConfig(null);
                }
              })
              .forPath(zookeeperNodeName);
    } catch (Exception e) {
      LOG.error("Error setting data watch on Metron config node", e);
    }
  }

  @Override
  public void configure(Config config) {
    zookeeperQuorum = Objects.requireNonNull(config.getString(ZOOKEEPER), "Zookeeper Quorum required");
    zookeeperNodeName = MoreObjects.firstNonNull(config.getString(ZOOKEEPER_NODE_NAME), DEFAULT_ENVELOPE_CONFIG_PATH);
  }
}
