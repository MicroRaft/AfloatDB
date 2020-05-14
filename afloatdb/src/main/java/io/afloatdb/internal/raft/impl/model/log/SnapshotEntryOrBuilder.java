/*
 * Copyright (c) 2020, AfloatDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.afloatdb.internal.raft.impl.model.log;

import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.afloatdb.raft.proto.SnapshotEntryProto;
import io.microraft.RaftEndpoint;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry;
import io.microraft.model.log.SnapshotEntry.SnapshotEntryBuilder;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

public class SnapshotEntryOrBuilder
        implements SnapshotEntry, SnapshotEntryBuilder {

    private SnapshotEntryProto.Builder builder;
    private SnapshotEntryProto entry;
    private List<SnapshotChunk> snapshotChunks;
    private Collection<RaftEndpoint> groupMembers;

    public SnapshotEntryOrBuilder() {
        this.builder = SnapshotEntryProto.newBuilder();
    }

    @Nonnull
    public SnapshotEntryProto getEntry() {
        return entry;
    }

    @Override
    public int getSnapshotChunkCount() {
        return entry.getSnapshotChunkCount();
    }

    @Override
    public long getGroupMembersLogIndex() {
        return entry.getGroupMembersLogIndex();
    }

    @Nonnull
    @Override
    public Collection<RaftEndpoint> getGroupMembers() {
        return groupMembers;
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder setIndex(long index) {
        builder.setIndex(index);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder setSnapshotChunks(@Nonnull List<SnapshotChunk> snapshotChunks) {
        snapshotChunks.stream().map(chunk -> ((SnapshotChunkOrBuilder) chunk).getSnapshotChunk())
                      .forEach(builder::addSnapshotChunk);
        this.snapshotChunks = snapshotChunks;
        return this;
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder setGroupMembersLogIndex(long groupMembersLogIndex) {
        builder.setGroupMembersLogIndex(groupMembersLogIndex);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder setGroupMembers(@Nonnull Collection<RaftEndpoint> groupMembers) {
        groupMembers.stream().map(AfloatDBEndpoint::extract).forEach(builder::addGroupMember);
        this.groupMembers = groupMembers;
        return this;
    }

    @Nonnull
    @Override
    public SnapshotEntry build() {
        entry = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "GrpcSnapshotEntryBuilder{builder=" + builder + "}";
        }

        return "GrpcSnapshotEntry{" + "index=" + getIndex() + ", term=" + getTerm() + ", operation=" + getOperation()
                + ", groupMembers=" + getGroupMembers() + ", groupMembersLogIndex=" + getGroupMembersLogIndex() + '}';
    }

    @Override
    public long getIndex() {
        return entry.getIndex();
    }

    @Override
    public int getTerm() {
        return entry.getTerm();
    }

    @Nonnull
    @Override
    public Object getOperation() {
        return snapshotChunks;
    }

}