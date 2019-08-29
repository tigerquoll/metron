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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

class ZookeeperClient {
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
