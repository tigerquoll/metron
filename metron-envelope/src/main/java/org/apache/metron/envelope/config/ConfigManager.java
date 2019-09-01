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
package org.apache.metron.envelope.config;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.ConfigurationType;
import org.apache.metron.common.configuration.Configurations;
import org.apache.metron.common.configuration.ParserConfigurations;
import org.apache.metron.common.configuration.writer.ConfigurationStrategy;
import org.apache.metron.common.configuration.writer.ConfigurationsStrategies;
import org.apache.metron.common.utils.LazyLogger;
import org.apache.metron.common.utils.LazyLoggerFactory;
import org.apache.metron.common.zookeeper.configurations.ConfigurationsUpdater;
import org.apache.metron.common.zookeeper.configurations.Reloadable;
import org.apache.metron.zookeeper.SimpleEventListener;
import org.apache.metron.zookeeper.ZKCache;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


import java.lang.invoke.MethodHandles;
import java.util.Objects;


/**
 * An class that manages specific configurations via ZooKeeper.
 * A cache is maintained using a {@link ZKCache}
 * Call init() before use, call close() when finished or use a resource protection block
 * to ensure resources are released
 *
 * @param <CONFIG_T> The config type being used, e.g. {@link ParserConfigurations}
 */
public abstract class ConfigManager<CONFIG_T extends Configurations> implements AutoCloseable, Reloadable {
  private static final LazyLogger LOG =  LazyLoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @NotNull private final String zookeeperUrl;
  @NotNull private final String configurationStrategy;

  protected CuratorFramework client;
  protected ZKCache cache;
  @NotNull private final CONFIG_T configurations;

  /**
   * Builds the bolt that knows where to communicate with ZooKeeper and which configuration this
   * bolt will be responsible for.
   *
   * @param zookeeperUrl A URL for ZooKeeper in the form host:port
   */
  public ConfigManager(String zookeeperUrl, String configurationStrategy) {
    this.zookeeperUrl = Objects.requireNonNull(zookeeperUrl);
    this.configurationStrategy = Objects.requireNonNull(configurationStrategy);
    this.configurations = Objects.requireNonNull(createUpdater().defaultConfigurations());
  }

  @NotNull
  protected ConfigurationStrategy<CONFIG_T> getConfigurationStrategy() {
    return ConfigurationsStrategies.valueOf(configurationStrategy);
  }

  @Nullable
  protected ConfigurationsUpdater<CONFIG_T> createUpdater() {
    return getConfigurationStrategy().createUpdater(this, this::getConfigurations);
  }

  public void setCuratorFramework(CuratorFramework client) {
    this.client = client;
  }

  public void setZKCache(ZKCache cache) {
    this.cache = cache;
  }

  @NotNull
  public CONFIG_T getConfigurations() {
    return configurations;
  }

  public void init() {
    prepCache();
  }

  @Override
  public void close() {
    cache.close();
    client.close();
  }

  /**
   * Prepares the cache that will be used during Metron's interaction with ZooKeeper.
   */
  protected void prepCache() {
    try {
      if (client == null) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(zookeeperUrl, retryPolicy);
      }
      client.start();

      //this is temporary to ensure that any validation passes.
      //The individual bolt will reinitialize stellar to dynamically pull from
      //zookeeper. ConfigurationsUtils.setupStellarStatically(client);
      if (cache == null) {
        ConfigurationsUpdater<CONFIG_T> updater = createUpdater();
        SimpleEventListener listener = new SimpleEventListener.Builder()
                                                              .with( updater::update
                                                                   , TreeCacheEvent.Type.NODE_ADDED
                                                                   , TreeCacheEvent.Type.NODE_UPDATED
                                                                   )
                                                              .with( updater::delete
                                                                   , TreeCacheEvent.Type.NODE_REMOVED
                                                                   )
                                                              .build();
        cache = new ZKCache.Builder()
                           .withClient(client)
                           .withListener(listener)
                           .withRoot(Constants.ZOOKEEPER_TOPOLOGY_ROOT)
                           .build();
        updater.forceUpdate(client);
        cache.start();
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reloadCallback(String name, ConfigurationType type) {
    LOG.info(String.format("Config update detected for %s of type %s - updated configuration will apply at next micro-batch", name, type));
  }
}
