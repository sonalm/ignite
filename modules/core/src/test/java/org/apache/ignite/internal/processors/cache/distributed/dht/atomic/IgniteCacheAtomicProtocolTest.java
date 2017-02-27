/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheAtomicProtocolTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String TEST_CACHE = "testCache";

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();
        commSpi.setIdleConnectionTimeout(1000);

        cfg.setCommunicationSpi(commSpi);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllPrimaryFailure1() throws Exception {
        putAllPrimaryFailure(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllPrimaryFailure2() throws Exception {
        putAllPrimaryFailure(true, true);
    }

    /**
     * @param fail0 Fail node 0 flag.
     * @param fail1 Fail node 1 flag.
     * @throws Exception If failed.
     */
    private void putAllPrimaryFailure(boolean fail0, boolean fail1) throws Exception {
        startGrids(4);

        client = true;

        Ignite client = startGrid(4);

        IgniteCache<Integer, Integer> nearCache = client.createCache(cacheConfiguration(1));
        IgniteCache<Integer, Integer> nearAsyncCache = nearCache.withAsync();

        awaitPartitionMapExchange();

        Ignite srv0 = ignite(0);
        Ignite srv1 = ignite(1);

        Integer key1 = primaryKey(srv0.cache(TEST_CACHE));
        Integer key2 = primaryKey(srv1.cache(TEST_CACHE));

        Map<Integer, Integer> map = new HashMap<>();
        map.put(key1, key1);
        map.put(key2, key2);

        assertEquals(2, map.size());

        if (fail0)
            testSpi(client).blockMessages(GridNearAtomicFullUpdateRequest.class, srv0.name());
        if (fail1)
            testSpi(client).blockMessages(GridNearAtomicFullUpdateRequest.class, srv1.name());

        log.info("Start put [key1=" + key1 + ", key2=" + key2 + ']');

        nearAsyncCache.putAll(map);

        IgniteFuture<?> fut = nearAsyncCache.future();

        U.sleep(500);

        assertFalse(fut.isDone());

        if (fail0)
            stopGrid(0);
        if (fail1)
            stopGrid(1);

        fut.get();

        checkData(map);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllBackupFailure1() throws Exception {
        startGrids(4);

        client = true;

        Ignite client = startGrid(4);

        IgniteCache<Integer, Integer> nearCache = client.createCache(cacheConfiguration(1));
        IgniteCache<Integer, Integer> nearAsyncCache = nearCache.withAsync();

        awaitPartitionMapExchange();

        Ignite srv0 = ignite(0);

        List<Integer> keys = primaryKeys(srv0.cache(TEST_CACHE), 3);

        Ignite backup = backup(client.affinity(TEST_CACHE), keys.get(0));

        testSpi(backup).blockMessages(GridDhtAtomicNearResponse.class, client.name());

        Map<Integer, Integer> map = new HashMap<>();

        for (Integer key : keys)
            map.put(key, key);

        log.info("Start put [map=" + map + ']');

        nearAsyncCache.putAll(map);

        IgniteFuture<?> fut = nearAsyncCache.future();

        U.sleep(500);

        assertFalse(fut.isDone());

        stopGrid(backup.name());

        fut.get();

        checkData(map);
    }

    /**
     * @param expData Expected cache data.
     */
    private void checkData(Map<Integer, Integer> expData) {
        List<Ignite> nodes = G.allGrids();

        assertFalse(nodes.isEmpty());

        for (Ignite node : nodes) {
            IgniteCache<Integer, Integer> cache = node.cache(TEST_CACHE);

            for (Map.Entry<Integer, Integer> e : expData.entrySet())
                assertEquals(e.getValue(), cache.get(e.getKey()));
        }
    }

    /**
     * @param aff Affinity.
     * @param key Key.
     * @return Backup node for given key.
     */
    private Ignite backup(Affinity<Object> aff, Object key) {
        for (Ignite ignite : G.allGrids()) {
            ClusterNode node = ignite.cluster().localNode();

            if (aff.isPrimaryOrBackup(node, key) && !aff.isPrimary(node, key))
                return ignite;
        }

        fail("Failed to find backup for key: " + key);

        return null;
    }

    /**
     * @param node Node.
     * @return Node communication SPI.
     */
    private TestRecordingCommunicationSpi testSpi(Ignite node) {
        return (TestRecordingCommunicationSpi)node.configuration().getCommunicationSpi();
    }

    /**
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(int backups) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setName(TEST_CACHE);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(backups);

        return ccfg;
    }
}
