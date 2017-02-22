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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import static org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateRequest.DHT_ATOMIC_HAS_RESULT_MASK;
import static org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateRequest.DHT_ATOMIC_PRIMARY_DHT_FAIL_RESPONSE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateRequest.DHT_ATOMIC_RESULT_SUCCESS_MASK;

/**
 * TODO IGNITE-4705: no not send mapping if it == affinity?
 */
public class GridDhtAtomicNearResponse extends GridCacheMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** */
    private int partId;

    /** */
    private long futId;

    /** */
    private UUID primaryId;

    /** */
    @GridDirectCollection(UUID.class)
    private List<UUID> mapping;

    /** */
    private byte flags;

    /** */
    private UpdateErrors errors;

    /** */
    private UUID failedNodeId;

    /**
     *
     */
    public GridDhtAtomicNearResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param partId Partition.
     * @param futId Future ID.
     * @param primaryId Primary node ID.
     * @param mapping Update mapping.
     * @param flags Flags.
     */
    public GridDhtAtomicNearResponse(int cacheId,
        int partId,
        long futId,
        UUID primaryId,
        List<UUID> mapping,
        byte flags)
    {
        this.cacheId = cacheId;
        this.partId = partId;
        this.futId = futId;
        this.primaryId = primaryId;
        this.mapping = mapping;
        this.flags = flags;
    }

    /**
     * @return Failed node ID.
     */
    UUID failedNodeId() {
        return failedNodeId;
    }

    /**
     * @param failedNodeId Failed node ID (used when primary notifies near node).
     */
    void failedNodeId(UUID failedNodeId) {
        assert failedNodeId != null;

        this.failedNodeId = failedNodeId;

        setFlag(true, DHT_ATOMIC_PRIMARY_DHT_FAIL_RESPONSE);
    }

    /**
     * @return {@code True} if message is sent from primary when DHT node fails.
     */
    boolean primaryDhtFailureResponse() {
        return isFlag(DHT_ATOMIC_PRIMARY_DHT_FAIL_RESPONSE);
    }

    /**
     * @return Primary node ID.
     */
    UUID primaryId() {
        return primaryId;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partId;
    }

    /**
     * @param key Key.
     * @param e Error.
     */
    public void addFailedKey(KeyCacheObject key, Throwable e) {
        if (errors == null)
            errors = new UpdateErrors();

        errors.addFailedKey(key, e);
    }

    /**
     * @return Operation result.
     */
    public GridCacheReturn result() {
        assert hasResult() : this;

        return new GridCacheReturn(true, isFlag(DHT_ATOMIC_RESULT_SUCCESS_MASK));
    }

    /**
     * @return {@code True} if response contains operation result.
     */
    boolean hasResult() {
        return isFlag(DHT_ATOMIC_HAS_RESULT_MASK);
    }

    /**
     * @return Update mapping.
     */
    public List<UUID> mapping() {
        return mapping;
    }

    /**
     * @param flag Set or clear.
     * @param mask Mask.
     */
    private void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reads flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    private boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -45;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (errors != null)
            errors.prepareMarshal(this, ctx.cacheContext(cacheId));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (errors != null)
            errors.finishUnmarshal(this, ctx.cacheContext(cacheId), ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeMessage("errors", errors))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeUuid("failedNodeId", failedNodeId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeCollection("mapping", mapping, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeInt("partId", partId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeUuid("primaryId", primaryId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 3:
                errors = reader.readMessage("errors");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                failedNodeId = reader.readUuid("failedNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                mapping = reader.readCollection("mapping", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                partId = reader.readInt("partId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                primaryId = reader.readUuid("primaryId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtAtomicNearResponse.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicNearResponse.class, this);
    }
}
