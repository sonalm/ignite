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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManager;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;

/**
 * Base for near atomic update futures.
 */
public abstract class GridNearAtomicAbstractUpdateFuture extends GridFutureAdapter<Object>
    implements GridCacheAtomicFuture<Object> {
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger log;

    /** Logger. */
    protected static IgniteLogger msgLog;

    /** Cache context. */
    protected final GridCacheContext cctx;

    /** Cache. */
    protected final GridDhtAtomicCache cache;

    /** Write synchronization mode. */
    protected final CacheWriteSynchronizationMode syncMode;

    /** Update operation. */
    protected final GridCacheOperation op;

    /** Optional arguments for entry processor. */
    protected final Object[] invokeArgs;

    /** Return value require flag. */
    protected final boolean retval;

    /** Raw return value flag. */
    protected final boolean rawRetval;

    /** Expiry policy. */
    protected final ExpiryPolicy expiryPlc;

    /** Optional filter. */
    protected final CacheEntryPredicate[] filter;

    /** Subject ID. */
    protected final UUID subjId;

    /** Task name hash. */
    protected final int taskNameHash;

    /** Skip store flag. */
    protected final boolean skipStore;

    /** Keep binary flag. */
    protected final boolean keepBinary;

    /** Wait for topology future flag. */
    protected final boolean waitTopFut;

    /** Near cache flag. */
    protected final boolean nearEnabled;

    /** Mutex to synchronize state updates. */
    protected final Object mux = new Object();

    /** Topology locked flag. Set if atomic update is performed inside a TX or explicit lock. */
    protected boolean topLocked;

    /** Remap count. */
    @GridToStringInclude
    protected int remapCnt;

    /** Current topology version. */
    @GridToStringInclude
    protected AffinityTopologyVersion topVer = AffinityTopologyVersion.ZERO;

    /** */
    @GridToStringInclude
    protected AffinityTopologyVersion remapTopVer;

    /** Error. */
    @GridToStringInclude
    protected CachePartialUpdateCheckedException err;

    /** Future ID. */
    @GridToStringInclude
    protected Long futId;

    /** Operation result. */
    protected GridCacheReturn opRes;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param cache Cache.
     * @param syncMode Synchronization mode.
     * @param op Operation.
     * @param invokeArgs Invoke arguments.
     * @param retval Return value flag.
     * @param rawRetval Raw return value flag.
     * @param expiryPlc Expiry policy.
     * @param filter Filter.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     * @param skipStore Skip store flag.
     * @param keepBinary Keep binary flag.
     * @param remapCnt Remap count.
     * @param waitTopFut Wait topology future flag.
     */
    protected GridNearAtomicAbstractUpdateFuture(
        GridCacheContext cctx,
        GridDhtAtomicCache cache,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        @Nullable Object[] invokeArgs,
        boolean retval,
        boolean rawRetval,
        @Nullable ExpiryPolicy expiryPlc,
        CacheEntryPredicate[] filter,
        UUID subjId,
        int taskNameHash,
        boolean skipStore,
        boolean keepBinary,
        int remapCnt,
        boolean waitTopFut
    ) {
        if (log == null) {
            msgLog = cctx.shared().atomicMessageLogger();
            log = U.logger(cctx.kernalContext(), logRef, GridFutureAdapter.class);
        }

        this.cctx = cctx;
        this.cache = cache;
        this.syncMode = syncMode;
        this.op = op;
        this.invokeArgs = invokeArgs;
        this.retval = retval;
        this.rawRetval = rawRetval;
        this.expiryPlc = expiryPlc;
        this.filter = filter;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.skipStore = skipStore;
        this.keepBinary = keepBinary;
        this.waitTopFut = waitTopFut;

        nearEnabled = CU.isNearEnabled(cctx);

        if (!waitTopFut)
            remapCnt = 1;

        this.remapCnt = remapCnt;
    }

    /**
     * Performs future mapping.
     */
    public final void map() {
        AffinityTopologyVersion topVer = cctx.shared().lockedTopologyVersion(null);

        if (topVer == null)
            mapOnTopology();
        else {
            topLocked = true;

            // Cannot remap.
            remapCnt = 1;

            Long futId = addAtomicFuture(topVer);

            if (futId != null)
                map(topVer, futId);
        }
    }

    /**
     * @param topVer Topology version.
     * @param futId Future ID.
     */
    protected abstract void map(AffinityTopologyVersion topVer, Long futId);

    /**
     * Maps future on ready topology.
     */
    protected abstract void mapOnTopology();

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // No-op.
    }

    /**
     * @return {@code True} future is stored by {@link GridCacheMvccManager#addAtomicFuture}.
     */
    protected boolean storeFuture() {
        return syncMode != FULL_ASYNC;
    }

    /**
     * Maps future to single node.
     *
     * @param nodeId Node ID.
     * @param req Request.
     */
    protected void mapSingle(UUID nodeId, GridNearAtomicAbstractUpdateRequest req) {
        if (cctx.localNodeId().equals(nodeId)) {
            cache.updateAllAsyncInternal(nodeId, req,
                new GridDhtAtomicCache.UpdateReplyClosure() {
                    @Override public void apply(GridNearAtomicAbstractUpdateRequest req, GridNearAtomicUpdateResponse res) {
                        onPrimaryResponse(res.nodeId(), res, false);
                    }
                });
        }
        else {
            try {
                cctx.io().send(req.nodeId(), req, cctx.ioPolicy());

                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near update fut, sent request [futId=" + req.futureId() +
                        ", writeVer=" + req.updateVersion() +
                        ", node=" + req.nodeId() + ']');
                }

                if (syncMode == FULL_ASYNC)
                    onDone(new GridCacheReturn(cctx, true, true, null, true));
            }
            catch (IgniteCheckedException e) {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near update fut, failed to send request [futId=" + req.futureId() +
                        ", writeVer=" + req.updateVersion() +
                        ", node=" + req.nodeId() +
                        ", err=" + e + ']');
                }

                onSendError(req, e);
            }
        }
    }

    /**
     * Response callback.
     *
     * @param nodeId Node ID.
     * @param res Update response.
     * @param nodeErr {@code True} if response was created on node failure.
     */
    public abstract void onPrimaryResponse(UUID nodeId, GridNearAtomicUpdateResponse res, boolean nodeErr);

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    public abstract void onDhtResponse(UUID nodeId, GridDhtAtomicNearResponse res);

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    public abstract void onMappingReceived(UUID nodeId, GridNearAtomicMappingResponse res);

    /**
     * @param req Request.
     * @param e Error.
     */
    protected final void onSendError(GridNearAtomicAbstractUpdateRequest req, IgniteCheckedException e) {
        synchronized (mux) {
            GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(cctx.cacheId(),
                req.nodeId(),
                req.futureId(),
                cctx.deploymentEnabled());

            res.addFailedKeys(req.keys(), e);

            onPrimaryResponse(req.nodeId(), res, true);
        }
    }

    /**
     * Adds future prevents topology change before operation complete.
     * Should be invoked before topology lock released.
     *
     * @param topVer Topology version.
     * @return Future ID in case future added.
     */
    final Long addAtomicFuture(AffinityTopologyVersion topVer) {
        // TODO IGNITE-4705: it seems no need to add future inside read lock.

        Long futId = cctx.mvcc().atomicFutureId();

        synchronized (mux) {
            assert this.futId == null : this;
            assert this.topVer == AffinityTopologyVersion.ZERO : this;

            this.topVer = topVer;
            this.futId = futId;
        }

        if (storeFuture() && !cctx.mvcc().addAtomicFuture(futId, this))
            return null;

        return futId;
    }

    /**
     *
     */
    static class PrimaryRequestState {
        /** */
        final GridNearAtomicAbstractUpdateRequest req;

        /** */
        private Set<UUID> rcvd;

        /** */
        @GridToStringInclude
        private Set<UUID> mapping;

        /** */
        private boolean hasRes;

        /**
         * @param req Request.
         */
        PrimaryRequestState(GridNearAtomicAbstractUpdateRequest req) {
            assert req != null && req.nodeId() != null : req;

            this.req = req;
        }

        /**
         * @return {@code True} if all expected responses are received.
         */
        private boolean finished() {
            return mapping != null && mapping.isEmpty() && hasRes;
        }

        /**
         * @param cctx Context.
         * @param nodeIds DHT nodes.
         */
        void initMapping(GridCacheContext cctx, List<UUID> nodeIds) {
            mapping = U.newHashSet(nodeIds.size());

            for (UUID dhtNodeId : nodeIds) {
                if (cctx.discovery().node(dhtNodeId) != null)
                    mapping.add(dhtNodeId);
            }

            if (rcvd != null)
                mapping.removeAll(rcvd);
        }

        /**
         * @param nodeId Node ID.
         * @return Request if need process primary response, {@code null} otherwise.
         */
        @Nullable GridNearAtomicAbstractUpdateRequest processPrimaryResponse(UUID nodeId) {
            if (finished())
                return null;

            if (req != null && req.nodeId().equals(nodeId) && req.response() == null)
                return req;

            return null;
        }

        /**
         * @param cctx Context.
         * @param res Response.
         * @return {@code True} if request processing finished.
         */
        boolean onMappingReceived(GridCacheContext cctx, GridNearAtomicMappingResponse res) {
            if (finished())
                return false;

            if (mapping == null) {
                initMapping(cctx, res.mapping());

                if (mapping.isEmpty() && hasRes)
                    return true;
            }

            return false;
        }

        /**
         * @param nodeId Node ID.
         * @return {@code True} if request processing finished.
         */
        boolean onNodeLeft(UUID nodeId) {
            if (finished())
                return false;

            if (mapping != null && mapping.remove(nodeId)) {
                if (mapping.isEmpty() && hasRes)
                    return true;
            }

            return false;
        }

        /**
         * TODO 4705: check response for errors.
         *
         * @param cctx Context.
         * @param nodeId Node ID.
         * @param res Response.
         * @return {@code True} if request processing finished.
         */
        boolean onDhtResponse(GridCacheContext cctx, UUID nodeId, GridDhtAtomicNearResponse res) {
            if (finished())
                return false;

            if (res.primaryDhtFailureResponse()) {
                assert res.mapping() != null : res;
                assert res.failedNodeId() != null : res;

                nodeId = res.failedNodeId();
            }

            if (res.mapping() != null) {
                // Mapping is sent from dht nodes.
                if (mapping == null)
                    initMapping(cctx, res.mapping());
            }
            else {
                // Mapping and result are sent from primary.
                if (mapping == null) {
                    if (rcvd == null)
                        rcvd = new HashSet<>();

                    rcvd.add(nodeId);

                    return false; // Need wait for response from primary.
                }
            }

            mapping.remove(nodeId);

            if (res.hasResult())
                hasRes = true;

            return mapping.isEmpty() && hasRes;
        }

        /**
         * @param cctx Context.
         * @param res Response.
         * @return {@code True} if request processing finished.
         */
        boolean onPrimaryResponse(GridCacheContext cctx, GridNearAtomicUpdateResponse res) {
            assert !finished() : this;

            hasRes = true;

            boolean onRes = req.onResponse(res);

            assert onRes;

            if (res.error() != null || res.remapTopologyVersion() != null)
                return true;

            assert res.returnValue() != null : res;

            if (res.mapping() != null)
                initMapping(cctx, res.mapping());
            else
                mapping = Collections.emptySet();

            return mapping.isEmpty();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PrimaryRequestState.class, this,
                "node", req.nodeId(),
                "rcvdRes", req.response() != null);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicAbstractUpdateFuture.class, this, super.toString());
    }
}
