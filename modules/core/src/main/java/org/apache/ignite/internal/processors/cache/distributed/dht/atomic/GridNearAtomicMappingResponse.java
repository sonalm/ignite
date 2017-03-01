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
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class GridNearAtomicMappingResponse extends GridCacheMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** */
    private static final int AFF_MAPPING_FLAG_MASK = 0x01;

    /** */
    private int part;

    /** */
    @GridDirectCollection(UUID.class)
    private List<UUID> mapping;

    /** */
    private long futId;

    /** */
    private byte flags;

    /**
     *
     */
    public GridNearAtomicMappingResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param part Partition.
     * @param futId Future ID.
     * @param mapping Mapping.
     * @param affMapping {@code True} if update mapping matches affinity function result.
     */
    GridNearAtomicMappingResponse(int cacheId, int part, long futId, List<UUID> mapping, boolean affMapping) {
        assert mapping == null || !affMapping;

        this.cacheId = cacheId;
        this.part = part;
        this.futId = futId;
        this.mapping = mapping;

        if (affMapping)
            setFlag(true, AFF_MAPPING_FLAG_MASK);
    }

    /**
     * @return {@code True} if update mapping matches affinity function result.
     */
    boolean affinityMapping() {
        return isFlag(AFF_MAPPING_FLAG_MASK);
    }

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    protected final void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reags flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    final boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return part;
    }

    /**
     * @return Mapping.
     */
    public List<UUID> mapping() {
        return mapping;
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
        return -47;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 7;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
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
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("mapping", mapping, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeInt("part", part))
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
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                mapping = reader.readCollection("mapping", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                part = reader.readInt("part");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicMappingResponse.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicMappingResponse.class, this);
    }
}
