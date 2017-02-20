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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * DHT atomic cache backup update future.
 */
public abstract class GridDhtAtomicAbstractUpdateFuture extends GridFutureAdapter<Void>
    implements GridCacheAtomicFuture<Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger. */
    protected static IgniteLogger log;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger msgLog;

    /** Write version. */
    protected final GridCacheVersion writeVer;

    /** Cache context. */
    protected final GridCacheContext cctx;

    /** Future version. */
    protected final long futId;

    /** Completion callback. */
    @GridToStringExclude
    private final GridDhtAtomicCache.UpdateReplyClosure completionCb;

    /** Update request. */
    protected final GridNearAtomicAbstractUpdateRequest updateReq;

    /** Update response. */
    final GridNearAtomicUpdateResponse updateRes;

    /** Mappings. */
    @GridToStringInclude
    protected Map<UUID, GridDhtAtomicAbstractUpdateRequest> mappings;

    /** Continuous query closures. */
    private Collection<CI1<Boolean>> cntQryClsrs;

    /** Response count. */
    private volatile int resCnt;

    /**
     * @param cctx Cache context.
     * @param completionCb Callback to invoke when future is completed.
     * @param writeVer Write version.
     * @param updateReq Update request.
     * @param updateRes Update response.
     */
    protected GridDhtAtomicAbstractUpdateFuture(
        GridCacheContext cctx,
        GridDhtAtomicCache.UpdateReplyClosure completionCb,
        GridCacheVersion writeVer,
        GridNearAtomicAbstractUpdateRequest updateReq,
        GridNearAtomicUpdateResponse updateRes) {
        this.cctx = cctx;
        this.updateReq = updateReq;
        this.completionCb = completionCb;
        this.updateRes = updateRes;
        this.writeVer = writeVer;

        futId = cctx.mvcc().atomicFutureId();

        if (log == null) {
            msgLog = cctx.shared().atomicMessageLogger();
            log = U.logger(cctx.kernalContext(), logRef, GridDhtAtomicUpdateFuture.class);
        }
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<Void> completeFuture(AffinityTopologyVersion topVer) {
        boolean waitForExchange = !updateReq.topologyLocked();

        if (waitForExchange && updateReq.topologyVersion().compareTo(topVer) < 0)
            return this;

        return null;
    }

    /**
     * @param clsr Continuous query closure.
     */
    public final void addContinuousQueryClosure(CI1<Boolean> clsr) {
        assert !isDone() : this;

        if (cntQryClsrs == null)
            cntQryClsrs = new ArrayList<>(10);

        cntQryClsrs.add(clsr);
    }

    /**
     * @param entry Entry to map.
     * @param val Value to write.
     * @param entryProcessor Entry processor.
     * @param ttl TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param addPrevVal If {@code true} sends previous value to backups.
     * @param prevVal Previous value.
     * @param updateCntr Partition update counter.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    final void addWriteEntry(
        UUID nearNodeId,
        GridDhtCacheEntry entry,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        @Nullable CacheObject prevVal,
        long updateCntr) {
        AffinityTopologyVersion topVer = updateReq.topologyVersion();

        List<ClusterNode> dhtNodes = cctx.dht().topology().nodes(entry.partition(), topVer);

        if (log.isDebugEnabled())
            log.debug("Mapping entry to DHT nodes [nodes=" + U.nodeIds(dhtNodes) + ", entry=" + entry + ']');

        CacheWriteSynchronizationMode syncMode = updateReq.writeSynchronizationMode();

        addDhtKey(entry.key(), dhtNodes);

        for (int i = 0; i < dhtNodes.size(); i++) {
            ClusterNode node = dhtNodes.get(i);

            UUID nodeId = node.id();

            if (!nodeId.equals(cctx.localNodeId())) {
                GridDhtAtomicAbstractUpdateRequest updateReq = mappings.get(nodeId);

                if (updateReq == null) {
                    updateReq = createRequest(
                        node.id(),
                        nearNodeId,
                        futId,
                        writeVer,
                        syncMode,
                        topVer,
                        ttl,
                        conflictExpireTime,
                        conflictVer);

                    mappings.put(nodeId, updateReq);
                }

                updateReq.addWriteValue(entry.key(),
                    val,
                    entryProcessor,
                    ttl,
                    conflictExpireTime,
                    conflictVer,
                    addPrevVal,
                    entry.partition(),
                    prevVal,
                    updateCntr);
            }
        }
    }

    /**
     * @param key Key.
     * @param dhtNodes DHT nodes.
     */
    protected abstract void addDhtKey(KeyCacheObject key, List<ClusterNode> dhtNodes);

    /**
     * @param key Key.
     * @param readers Near cache readers.
     */
    protected abstract void addNearKey(KeyCacheObject key, Collection<UUID> readers);

    /**
     * @param readers Entry readers.
     * @param entry Entry.
     * @param val Value.
     * @param entryProcessor Entry processor..
     * @param ttl TTL for near cache update (optional).
     * @param expireTime Expire time for near cache update (optional).
     */
    final void addNearWriteEntries(
        UUID nearNodeId,
        Collection<UUID> readers,
        GridDhtCacheEntry entry,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long expireTime) {
        CacheWriteSynchronizationMode syncMode = updateReq.writeSynchronizationMode();

        addNearKey(entry.key(), readers);

        AffinityTopologyVersion topVer = updateReq.topologyVersion();

        for (UUID nodeId : readers) {
            GridDhtAtomicAbstractUpdateRequest updateReq = mappings.get(nodeId);

            if (updateReq == null) {
                ClusterNode node = cctx.discovery().node(nodeId);

                // Node left the grid.
                if (node == null)
                    continue;

                updateReq = createRequest(
                    node.id(),
                    nearNodeId,
                    futId,
                    writeVer,
                    syncMode,
                    topVer,
                    ttl,
                    expireTime,
                    null);

                mappings.put(nodeId, updateReq);
            }

            addNearReaderEntry(entry);

            updateReq.addNearWriteValue(entry.key(),
                val,
                entryProcessor,
                ttl,
                expireTime);
        }
    }

    /**
     * adds new nearReader.
     *
     * @param entry GridDhtCacheEntry.
     */
    protected abstract void addNearReaderEntry(GridDhtCacheEntry entry);

    /**
     * @return Write version.
     */
    final GridCacheVersion writeVersion() {
        return writeVer;
    }

    /** {@inheritDoc} */
    @Override public final IgniteUuid futureId() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public final Long id() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public final boolean onNodeLeft(UUID nodeId) {
        boolean res = registerResponse(nodeId, true);

        if (res && msgLog.isDebugEnabled()) {
            msgLog.debug("DTH update fut, node left [futId=" + futId + ", writeVer=" + writeVer +
                ", node=" + nodeId + ']');
        }

        return res;
    }

    /**
     * @param nodeId Node ID.
     * @param nodeErr Node error flag.
     * @return {@code True} if request found.
     */
    final boolean registerResponse(UUID nodeId, boolean nodeErr) {
        int resCnt0;

        GridDhtAtomicAbstractUpdateRequest req = mappings != null ? mappings.get(nodeId) : null;

        if (req != null) {
            synchronized (this) {
                if (req.onResponse()) {
                    resCnt0 = resCnt;

                    resCnt0 += 1;

                    resCnt = resCnt0;
                }
                else
                    return false;
            }

            if (resCnt0 == mappings.size())
                onDone();

            return true;
        }

        return false;
    }

    /**
     * Sends requests to remote nodes.
     *
     * @param ret Cache operation return value.
     */
    final void map(GridCacheReturn ret) {
        boolean fullSync = updateReq.writeSynchronizationMode() == FULL_SYNC;
        boolean primaryReplyToNear = updateReq.writeSynchronizationMode() == PRIMARY_SYNC || ret.hasValue();

        List<UUID> dhtNodes = null;

        if (fullSync) {
            if (!F.isEmpty(mappings)) {
                dhtNodes = new ArrayList<>(mappings.size());

                dhtNodes.addAll(mappings.keySet());
            }
            else
                dhtNodes = Collections.emptyList();

            if (primaryReplyToNear)
                updateRes.mapping(dhtNodes);
        }

        if (!F.isEmpty(mappings)) {
            sendDhtRequests(fullSync && !primaryReplyToNear, dhtNodes, ret);

            if (primaryReplyToNear)
                completionCb.apply(updateReq, updateRes);
            else {
                if (fullSync && GridDhtAtomicCache.IGNITE_ATOMIC_SND_MAPPING_TO_NEAR) {
                    GridNearAtomicMappingResponse mappingRes = new GridNearAtomicMappingResponse(
                        cctx.cacheId(),
                        updateReq.partition(),
                        updateReq.futureId(),
                        dhtNodes);

                    try {
                        cctx.io().send(updateRes.nodeId(), mappingRes, cctx.ioPolicy());
                    }
                    catch (IgniteCheckedException e) {
                        U.error(msgLog, "Failed to send mapping response [futId=" + futId +
                            ", writeVer=" + writeVer + ", node=" + updateRes.nodeId() + ']');
                    }
                }
            }
        }
        else {
            completionCb.apply(updateReq, updateRes);

            onDone();
        }
    }

    /**
     * @param nearReplyInfo {@code True} if need add inforamtion for near node response.
     * @param dhtNodes DHT nodes.
     * @param ret Return value.
     */
    private void sendDhtRequests(boolean nearReplyInfo, List<UUID> dhtNodes, GridCacheReturn ret) {
        for (GridDhtAtomicAbstractUpdateRequest req : mappings.values()) {
            try {
                if (nearReplyInfo) {
                    req.dhtNodes(dhtNodes);

                    if (!ret.hasValue())
                        req.setResult(ret.success());
                }

                cctx.io().send(req.nodeId(), req, cctx.ioPolicy());

                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("DTH update fut, sent request [futId=" + futId +
                        ", writeVer=" + writeVer + ", node=" + req.nodeId() + ']');
                }
            }
            catch (ClusterTopologyCheckedException ignored) {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("DTH update fut, failed to send request, node left [futId=" + futId +
                        ", writeVer=" + writeVer + ", node=" + req.nodeId() + ']');
                }

                registerResponse(req.nodeId(), true);
            }
            catch (IgniteCheckedException ignored) {
                U.error(msgLog, "Failed to send request [futId=" + futId +
                    ", writeVer=" + writeVer + ", node=" + req.nodeId() + ']');

                registerResponse(req.nodeId(), true);
            }
        }
    }

    /**
     * Deferred update response.
     *
     * @param nodeId Backup node ID.
     */
    public final void onResult(UUID nodeId) {
        if (log.isDebugEnabled())
            log.debug("Received deferred DHT atomic update future result [nodeId=" + nodeId + ']');

        registerResponse(nodeId, false);
    }

    /**
     * @param nodeId Node ID.
     * @param nearNodeId Near node ID.
     * @param futId Future ID.
     * @param writeVer Update version.
     * @param syncMode Write synchronization mode.
     * @param topVer Topology version.
     * @param ttl TTL.
     * @param conflictExpireTime Conflict expire time.
     * @param conflictVer Conflict version.
     * @return Request.
     */
    protected abstract GridDhtAtomicAbstractUpdateRequest createRequest(
        UUID nodeId,
        UUID nearNodeId,
        long futId,
        GridCacheVersion writeVer,
        CacheWriteSynchronizationMode syncMode,
        @NotNull AffinityTopologyVersion topVer,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer
    );

    /**
     * Callback for backup update response.
     *
     * @param nodeId Backup node ID.
     * @param updateRes Update response.
     */
    public abstract void onResult(UUID nodeId, GridDhtAtomicUpdateResponse updateRes);

    /**
     * @param updateRes Response.
     * @param err Error.
     */
    protected abstract void addFailedKeys(GridNearAtomicUpdateResponse updateRes, Throwable err);

    /** {@inheritDoc} */
    @Override public final boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            cctx.mvcc().removeAtomicFuture(futId);

            boolean suc = err == null;

            if (!suc)
                addFailedKeys(updateRes, err);

            if (cntQryClsrs != null) {
                for (CI1<Boolean> clsr : cntQryClsrs)
                    clsr.apply(suc);
            }

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // No-op.
    }
}
