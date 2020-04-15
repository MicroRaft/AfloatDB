package io.afloatdb.internal.raft.impl.model.log;

import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.afloatdb.raft.proto.ProtoKVSnapshotChunk;
import io.afloatdb.raft.proto.ProtoKVSnapshotChunkObject;
import io.microraft.RaftEndpoint;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotChunk.SnapshotChunkBuilder;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.LinkedHashSet;

public class GrpcSnapshotChunkOrBuilder
        implements SnapshotChunk, SnapshotChunkBuilder {

    private ProtoKVSnapshotChunk.Builder builder;
    private ProtoKVSnapshotChunk snapshotChunk;
    private Collection<RaftEndpoint> groupMembers;

    public GrpcSnapshotChunkOrBuilder() {
        this.builder = ProtoKVSnapshotChunk.newBuilder();
    }

    public GrpcSnapshotChunkOrBuilder(ProtoKVSnapshotChunk snapshotChunk) {
        this.snapshotChunk = snapshotChunk;
        this.groupMembers = new LinkedHashSet<>();
        snapshotChunk.getGroupMemberList().stream().map(AfloatDBEndpoint::wrap).forEach(groupMembers::add);
    }

    public ProtoKVSnapshotChunk getSnapshotChunk() {
        return snapshotChunk;
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder setIndex(long index) {
        builder.setIndex(index);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder setOperation(@Nonnull Object operation) {
        builder.setOperation((ProtoKVSnapshotChunkObject) operation);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder setSnapshotChunkIndex(int snapshotChunkIndex) {
        builder.setSnapshotChunkIndex(snapshotChunkIndex);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder setSnapshotChunkCount(int snapshotChunkCount) {
        builder.setSnapshotChunkCount(snapshotChunkCount);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder setGroupMembersLogIndex(long groupMembersLogIndex) {
        builder.setGroupMembersLogIndex(groupMembersLogIndex);
        return this;
    }

    @Nonnull
    @Override
    public SnapshotChunkBuilder setGroupMembers(@Nonnull Collection<RaftEndpoint> groupMembers) {
        groupMembers.stream().map(AfloatDBEndpoint::extract).forEach(builder::addGroupMember);
        this.groupMembers = groupMembers;
        return this;
    }

    @Nonnull
    @Override
    public SnapshotChunk build() {
        snapshotChunk = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "GrpcSnapshotChunkBuilder{builder=" + builder + "}";
        }

        return "GrpcSnapshotChunk{" + "index=" + getIndex() + ", term=" + getTerm() + ", operation=" + getOperation()
                + ", snapshotChunkIndex=" + getSnapshotChunkIndex() + ", snapshotChunkCount=" + getSnapshotChunkCount()
                + ", groupMembers=" + getGroupMembers() + ", groupMembersLogIndex=" + getGroupMembersLogIndex() + '}';
    }

    @Override
    public int getSnapshotChunkIndex() {
        return snapshotChunk.getSnapshotChunkIndex();
    }

    @Override
    public int getSnapshotChunkCount() {
        return snapshotChunk.getSnapshotChunkCount();
    }

    @Override
    public long getGroupMembersLogIndex() {
        return snapshotChunk.getGroupMembersLogIndex();
    }

    @Nonnull
    @Override
    public Collection<RaftEndpoint> getGroupMembers() {
        return groupMembers;
    }

    @Override
    public long getIndex() {
        return snapshotChunk.getIndex();
    }

    @Override
    public int getTerm() {
        return snapshotChunk.getTerm();
    }

    @Nonnull
    @Override
    public Object getOperation() {
        return snapshotChunk.getOperation();
    }

}
