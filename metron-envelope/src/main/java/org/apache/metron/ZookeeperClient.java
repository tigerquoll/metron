package org.apache.metron;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZookeeperClient {
  private static final int ZK_EXP_BACKOFF_RETRY_MS = 1000;
  private static final int ZK_EXP_BACKOFF_RETRIES = 3;

  private static volatile CuratorFramework zkClient;

  static CuratorFramework getZKInstance(final String zookeeperQuorum) {
    if (zkClient == null) {
      synchronized (CuratorFramework.class) {
        if (zkClient == null) {
          zkClient = CuratorFrameworkFactory.newClient(zookeeperQuorum,
                  new ExponentialBackoffRetry(ZK_EXP_BACKOFF_RETRY_MS, ZK_EXP_BACKOFF_RETRIES));
          zkClient.start();
        }
      }
    }
    return zkClient;
  }


}
