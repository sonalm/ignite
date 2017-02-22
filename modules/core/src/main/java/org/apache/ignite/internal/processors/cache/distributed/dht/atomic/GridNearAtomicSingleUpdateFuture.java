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
import java.util.Map;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.EntryProcessorResourceInjectorProxy;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheTryPutFailedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearAtomicCache;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;

/**
 * DHT atomic cache near update future.
 */
public class GridNearAtomicSingleUpdateFuture extends GridNearAtomicAbstractUpdateFuture {
    /** Keys */
    private Object key;

    /** Values. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private Object val;

    /** */
    private PrimaryRequestState reqState;

    /**
     * @param cctx Cache context.
     * @param cache Cache instance.
     * @param syncMode Write synchronization mode.
     * @param op Update operation.
     * @param key Keys to update.
     * @param val Values or transform closure.
     * @param invokeArgs Optional arguments for entry processor.
     * @param retval Return value require flag.
     * @param rawRetval {@code True} if should return {@code GridCacheReturn} as future result.
     * @param expiryPlc Expiry policy explicitly specified for cache operation.
     * @param filter Entry filter.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param skipStore Skip store flag.
     * @param keepBinary Keep binary flag.
     * @param remapCnt Maximum number of retries.
     * @param waitTopFut If {@code false} does not wait for affinity change future.
     */
    public GridNearAtomicSingleUpdateFuture(
        GridCacheContext cctx,
        GridDhtAtomicCache cache,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        Object key,
        @Nullable Object val,
        @Nullable Object[] invokeArgs,
        final boolean retval,
        final boolean rawRetval,
        @Nullable ExpiryPolicy expiryPlc,
        final CacheEntryPredicate[] filter,
        UUID subjId,
        int taskNameHash,
        boolean skipStore,
        boolean keepBinary,
        int remapCnt,
        boolean waitTopFut
    ) {
        super(cctx, cache, syncMode, op, invokeArgs, retval, rawRetval, expiryPlc, filter, subjId, taskNameHash,
            skipStore, keepBinary, remapCnt, waitTopFut);

        assert subjId != null;

        this.key = key;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public Long id() {
        synchronized (mux) {
            return futId;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        GridNearAtomicAbstractUpdateRequest req;

        GridNearAtomicUpdateResponse res = null;

        GridCacheReturn opRes0 = null;
        CachePartialUpdateCheckedException err0 = null;

        synchronized (mux) {
            if (reqState == null)
                return false;

            req = reqState.processPrimaryResponse(nodeId);

            if (req != null) {
                res = new GridNearAtomicUpdateResponse(cctx.cacheId(),
                    nodeId,
                    req.futureId(),
                    cctx.deploymentEnabled());

                ClusterTopologyCheckedException e = new ClusterTopologyCheckedException("Primary node left grid " +
                    "before response is received: " + nodeId);

                e.retryReadyFuture(cctx.shared().nextAffinityReadyFuture(req.topologyVersion()));

                res.addFailedKeys(req.keys(), e);
            }
            else {
                if (reqState.onNodeLeft(nodeId)) {
                    opRes0 = opRes;
                    err0 = err;
                }
                else
                    return false;
            }
        }

        if (res != null) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Near update single fut, node left [futId=" + req.futureId() +
                    ", writeVer=" + req.updateVersion() +
                    ", node=" + nodeId + ']');
            }

            onPrimaryResponse(nodeId, res, true);
        }
        else
            onDone(opRes0, err0);

        return false;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> completeFuture(AffinityTopologyVersion topVer) {
        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public boolean onDone(@Nullable Object res, @Nullable Throwable err) {
        assert res == null || res instanceof GridCacheReturn;

        GridCacheReturn ret = (GridCacheReturn)res;

        Object retval = res == null ? null : rawRetval ? ret : (this.retval || op == TRANSFORM) ?
                cctx.unwrapBinaryIfNeeded(ret.value(), keepBinary) : ret.success();

        if (op == TRANSFORM && retval == null)
            retval = Collections.emptyMap();

        if (super.onDone(retval, err)) {
            Long futVer = onFutureDone();

            if (futVer != null)
                cctx.mvcc().removeAtomicFuture(futVer);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void onMappingReceived(UUID nodeId, GridNearAtomicMappingResponse res) {
        GridCacheReturn opRes0;
        CachePartialUpdateCheckedException err0;

        synchronized (mux) {
            if (futId == null || futId != res.futureId())
                return;

            assert reqState != null;

            if (reqState.onMappingReceived(cctx, res)) {
                opRes0 = opRes;
                err0 = err;
            }
            else
                return;
        }

        onDone(opRes0, err0);
    }

    /** {@inheritDoc} */
    @Override public void onDhtResponse(UUID nodeId, GridDhtAtomicNearResponse res) {
        GridCacheReturn opRes0;
        CachePartialUpdateCheckedException err0;

        synchronized (mux) {
            if (futId == null || futId != res.futureId())
                return;

            assert reqState != null;
            assert reqState.req.nodeId().equals(res.primaryId());

            if (opRes == null && res.hasResult())
                opRes = res.result();

            if (reqState.onDhtResponse(cctx, nodeId, res)) {
                opRes0 = opRes;
                err0 = err;
            }
            else
                return;
        }

        onDone(opRes0, err0);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
    @Override public void onPrimaryResponse(UUID nodeId, GridNearAtomicUpdateResponse res, boolean nodeErr) {
        GridNearAtomicAbstractUpdateRequest req;

        AffinityTopologyVersion remapTopVer = null;

        GridCacheReturn opRes0 = null;
        CachePartialUpdateCheckedException err0 = null;

        synchronized (mux) {
            if (futId == null || futId != res.futureId())
                return;

            req = reqState.processPrimaryResponse(nodeId);

            if (req == null)
                return;

            boolean remapKey = !F.isEmpty(res.remapKeys());

            if (remapKey) {
                if (mapErrTopVer == null || mapErrTopVer.compareTo(req.topologyVersion()) < 0)
                    mapErrTopVer = req.topologyVersion();
            }
            else if (res.error() != null) {
                // TODO IGNITE-4705: assert only 1 key?
                if (res.failedKeys() != null) {
                    if (err == null)
                        err = new CachePartialUpdateCheckedException(
                            "Failed to update keys (retry update if possible).");

                    Collection<Object> keys = new ArrayList<>(res.failedKeys().size());

                    for (KeyCacheObject key : res.failedKeys())
                        keys.add(cctx.cacheObjectContext().unwrapBinaryIfNeeded(key, keepBinary, false));

                    err.add(keys, res.error(), req.topologyVersion());
                }
            }
            else {
                GridCacheReturn ret = res.returnValue();

                if (op == TRANSFORM) {
                    if (ret != null) {
                        assert ret.value() == null || ret.value() instanceof Map : ret.value();

                        if (ret.value() != null) {
                            if (opRes != null)
                                opRes.mergeEntryProcessResults(ret);
                            else
                                opRes = ret;
                        }
                    }
                }
                else
                    opRes = ret;

                assert reqState != null;

                if (!reqState.onPrimaryResponse(cctx, res))
                    return;
            }

            if (remapKey) {
                assert mapErrTopVer != null;

                remapTopVer = cctx.shared().exchange().topologyVersion();
            }
            else {
                if (err != null &&
                    X.hasCause(err, CachePartialUpdateCheckedException.class) &&
                    X.hasCause(err, ClusterTopologyCheckedException.class) &&
                    storeFuture() &&
                    --remapCnt > 0) {
                    ClusterTopologyCheckedException topErr = X.cause(err, ClusterTopologyCheckedException.class);

                    if (!(topErr instanceof ClusterTopologyServerNotFoundException)) {
                        CachePartialUpdateCheckedException cause =
                            X.cause(err, CachePartialUpdateCheckedException.class);

                        assert cause != null && cause.topologyVersion() != null : err;

                        remapTopVer =
                            new AffinityTopologyVersion(cause.topologyVersion().topologyVersion() + 1);

                        err = null;
                    }
                }
            }

            if (remapTopVer == null) {
                err0 = err;
                opRes0 = opRes;
            }
            else {
                cctx.mvcc().removeAtomicFuture(futId);

                reqState = null;
                futId = null;
                topVer = AffinityTopologyVersion.ZERO;
            }
        }

        if (res.error() != null && res.failedKeys() == null) {
            onDone(res.error());

            return;
        }

        if (nearEnabled && !nodeErr)
            updateNear(req, res);

        if (remapTopVer != null) {
            if (!waitTopFut) {
                onDone(new GridCacheTryPutFailedException());

                return;
            }

            if (topLocked) {
                CachePartialUpdateCheckedException e =
                    new CachePartialUpdateCheckedException("Failed to update keys (retry update if possible).");

                ClusterTopologyCheckedException cause = new ClusterTopologyCheckedException(
                    "Failed to update keys, topology changed while execute atomic update inside transaction.");

                cause.retryReadyFuture(cctx.affinity().affinityReadyFuture(remapTopVer));

                e.add(Collections.singleton(cctx.toCacheKeyObject(key)), cause);

                onDone(e);

                return;
            }

            IgniteInternalFuture<AffinityTopologyVersion> fut =
                cctx.shared().exchange().affinityReadyFuture(remapTopVer);

            if (fut == null)
                fut = new GridFinishedFuture<>(remapTopVer);

            fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(final IgniteInternalFuture<AffinityTopologyVersion> fut) {
                    cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                        @Override public void run() {
                            mapOnTopology();
                        }
                    });
                }
            });

            return;
        }

        onDone(opRes0, err0);
    }

    /**
     * Updates near cache.
     *
     * @param req Update request.
     * @param res Update response.
     */
    private void updateNear(GridNearAtomicAbstractUpdateRequest req, GridNearAtomicUpdateResponse res) {
        assert nearEnabled;

        if (res.remapKeys() != null || !req.hasPrimary())
            return;

        GridNearAtomicCache near = (GridNearAtomicCache)cctx.dht().near();

        near.processNearAtomicUpdateResponse(req, res);
    }

    /** {@inheritDoc} */
    @Override protected void mapOnTopology() {
        // TODO IGNITE-4705: primary should block topology change, so it seems read lock is not needed.
        cache.topology().readLock();

        AffinityTopologyVersion topVer;
        Long futId;

        try {
            if (cache.topology().stopping()) {
                onDone(new IgniteCheckedException("Failed to perform cache operation (cache is stopped): " +
                    cache.name()));

                return;
            }

            GridDhtTopologyFuture fut = cache.topology().topologyVersionFuture();

            if (fut.isDone()) {
                Throwable err = fut.validateCache(cctx);

                if (err != null) {
                    onDone(err);

                    return;
                }

                topVer = fut.topologyVersion();

                futId = addAtomicFuture(topVer);
            }
            else {
                if (waitTopFut) {
                    assert !topLocked : this;

                    fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                            cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                                @Override public void run() {
                                    mapOnTopology();
                                }
                            });
                        }
                    });
                }
                else
                    onDone(new GridCacheTryPutFailedException());

                return;
            }
        }
        finally {
            cache.topology().readUnlock();
        }

        if (futId != null)
            map(topVer, futId);
    }

    /** {@inheritDoc} */
    @Override protected void map(AffinityTopologyVersion topVer, Long futId) {
        Exception err = null;
        GridNearAtomicAbstractUpdateRequest singleReq0 = null;

        try {
            singleReq0 = mapSingleUpdate(topVer, futId);

            synchronized (mux) {
                assert this.futId.equals(futId) || (this.isDone() && this.error() != null);
                assert this.topVer == topVer;

                resCnt = 0;

                reqState = new PrimaryRequestState(singleReq0);
            }
        }
        catch (Exception e) {
            err = e;
        }

        if (err != null) {
            onDone(err);

            return;
        }

        // Optimize mapping for single key.
        mapSingle(singleReq0.nodeId(), singleReq0);
    }

    /**
     * @return Future ID.
     */
    private Long onFutureDone() {
        Long id0;

        synchronized (mux) {
            id0 = futId;

            futId = null;
        }

        return id0;
    }

    /**
     * @param topVer Topology version.
     * @param futId Future ID.
     * @return Request.
     * @throws Exception If failed.
     */
    private GridNearAtomicAbstractUpdateRequest mapSingleUpdate(AffinityTopologyVersion topVer, long futId)
        throws Exception {
        if (key == null)
            throw new NullPointerException("Null key.");

        Object val = this.val;

        if (val == null && op != GridCacheOperation.DELETE)
            throw new NullPointerException("Null value.");

        KeyCacheObject cacheKey = cctx.toCacheKeyObject(key);

        if (op != TRANSFORM)
            val = cctx.toCacheObject(val);
        else
            val = EntryProcessorResourceInjectorProxy.wrap(cctx.kernalContext(), (EntryProcessor)val);

        ClusterNode primary = cctx.affinity().primaryByKey(cacheKey, topVer);

        if (primary == null)
            throw new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all partition nodes " +
                "left the grid).");

        GridNearAtomicAbstractUpdateRequest req;

        if (canUseSingleRequest()) {
            if (op == TRANSFORM) {
                req = new GridNearAtomicSingleUpdateInvokeRequest(
                    cctx.cacheId(),
                    primary.id(),
                    futId,
                    false,
                    null,
                    topVer,
                    topLocked,
                    syncMode,
                    op,
                    retval,
                    invokeArgs,
                    subjId,
                    taskNameHash,
                    skipStore,
                    keepBinary,
                    cctx.kernalContext().clientNode(),
                    cctx.deploymentEnabled());
            }
            else {
                if (filter == null || filter.length == 0) {
                    req = new GridNearAtomicSingleUpdateRequest(
                        cctx.cacheId(),
                        primary.id(),
                        futId,
                        false,
                        null,
                        topVer,
                        topLocked,
                        syncMode,
                        op,
                        retval,
                        subjId,
                        taskNameHash,
                        skipStore,
                        keepBinary,
                        cctx.kernalContext().clientNode(),
                        cctx.deploymentEnabled());
                }
                else {
                    req = new GridNearAtomicSingleUpdateFilterRequest(
                        cctx.cacheId(),
                        primary.id(),
                        futId,
                        false,
                        null,
                        topVer,
                        topLocked,
                        syncMode,
                        op,
                        retval,
                        filter,
                        subjId,
                        taskNameHash,
                        skipStore,
                        keepBinary,
                        cctx.kernalContext().clientNode(),
                        cctx.deploymentEnabled());
                }
            }
        }
        else {
            req = new GridNearAtomicFullUpdateRequest(
                cctx.cacheId(),
                primary.id(),
                futId,
                false,
                null,
                topVer,
                topLocked,
                syncMode,
                op,
                retval,
                expiryPlc,
                invokeArgs,
                filter,
                subjId,
                taskNameHash,
                skipStore,
                keepBinary,
                cctx.kernalContext().clientNode(),
                cctx.deploymentEnabled(),
                1);
        }

        req.addUpdateEntry(cacheKey,
            val,
            CU.TTL_NOT_CHANGED,
            CU.EXPIRE_TIME_CALCULATE,
            null,
            true);

        return req;
    }

    /**
     * @return {@code True} can use 'single' update requests.
     */
    private boolean canUseSingleRequest() {
        return expiryPlc == null;
    }

    /** {@inheritDoc} */
    public String toString() {
        synchronized (mux) {
            return S.toString(GridNearAtomicSingleUpdateFuture.class, this, super.toString());
        }
    }
}
