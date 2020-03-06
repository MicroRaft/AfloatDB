package io.afloatdb.internal.raft.impl.model.message;

import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.afloatdb.raft.proto.ProtoInstallSnapshotResponse;
import io.afloatdb.raft.proto.ProtoRaftMessage;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.InstallSnapshotResponse;
import io.microraft.model.message.InstallSnapshotResponse.InstallSnapshotResponseBuilder;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class GrpcInstallSnapshotResponseOrBuilder
        implements InstallSnapshotResponse, InstallSnapshotResponseBuilder, GrpcRaftMessage {

    private ProtoInstallSnapshotResponse.Builder builder;
    private ProtoInstallSnapshotResponse response;
    private RaftEndpoint sender;

    public GrpcInstallSnapshotResponseOrBuilder() {
        this.builder = ProtoInstallSnapshotResponse.newBuilder();
    }

    public GrpcInstallSnapshotResponseOrBuilder(ProtoInstallSnapshotResponse response) {
        this.response = response;
        this.sender = AfloatDBEndpoint.wrap(response.getSender());
    }

    public ProtoInstallSnapshotResponse getResponse() {
        return response;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setGroupId(@Nonnull Object groupId) {
        requireNonNull(groupId);
        builder.setGroupId((String) groupId);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setSender(@Nonnull RaftEndpoint sender) {
        builder.setSender(AfloatDBEndpoint.extract(sender));
        this.sender = sender;
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setSnapshotIndex(long snapshotIndex) {
        builder.setSnapshotIndex(snapshotIndex);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setRequestedSnapshotChunkIndices(@Nonnull List<Integer> requestedSnapshotChunkIndices) {
        requireNonNull(requestedSnapshotChunkIndices);
        builder.addAllRequestedSnapshotChunkIndices(requestedSnapshotChunkIndices);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setQueryRound(long queryRound) {
        builder.setQueryRound(queryRound);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponse build() {
        response = builder.build();
        builder = null;
        return this;
    }

    @Override
    public void populate(ProtoRaftMessage.Builder builder) {
        builder.setInstallSnapshotResponse(response);
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "GrpcInstallSnapshotResponseBuilder{builder=" + builder + "}";
        }

        return "GrpcInstallSnapshotResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term=" + getTerm()
                + ", snapshotIndex=" + getSnapshotIndex() + ", requestedSnapshotChunkIndices="
                + getRequestedSnapshotChunkIndices() + ", queryRound=" + getQueryRound() + '}';
    }

    @Override
    public long getSnapshotIndex() {
        return response.getSnapshotIndex();
    }

    @Nonnull
    @Override
    public List<Integer> getRequestedSnapshotChunkIndices() {
        return response.getRequestedSnapshotChunkIndicesList();
    }

    @Override
    public long getQueryRound() {
        return response.getQueryRound();
    }

    @Override
    public Object getGroupId() {
        return response.getGroupId();
    }

    @Nonnull
    @Override
    public RaftEndpoint getSender() {
        return sender;
    }

    @Override
    public int getTerm() {
        return response.getTerm();
    }

}
