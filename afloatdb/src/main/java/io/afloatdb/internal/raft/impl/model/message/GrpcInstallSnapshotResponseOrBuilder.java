package io.afloatdb.internal.raft.impl.model.message;

import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.afloatdb.raft.proto.ProtoInstallSnapshotResponse;
import io.afloatdb.raft.proto.ProtoRaftMessage;
import io.microraft.RaftEndpoint;
import io.microraft.model.message.InstallSnapshotResponse;
import io.microraft.model.message.InstallSnapshotResponse.InstallSnapshotResponseBuilder;

import javax.annotation.Nonnull;

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
    public InstallSnapshotResponseBuilder setRequestedSnapshotChunkIndex(int requestedSnapshotChunkIndex) {
        builder.setRequestedSnapshotChunkIndex(requestedSnapshotChunkIndex);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setQuerySeqNo(long querySeqNo) {
        builder.setQuerySeqNo(querySeqNo);
        return this;
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder setFlowControlSeqNo(long flowControlSeqNo) {
        builder.setFlowControlSeqNo(flowControlSeqNo);
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
    public long getSnapshotIndex() {
        return response.getSnapshotIndex();
    }

    @Override
    public int getRequestedSnapshotChunkIndex() {
        return response.getRequestedSnapshotChunkIndex();
    }

    @Override
    public long getQuerySeqNo() {
        return response.getQuerySeqNo();
    }

    @Override
    public long getFlowControlSeqNo() {
        return response.getFlowControlSeqNo();
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

    @Override
    public String toString() {
        if (builder != null) {
            return "GrpcInstallSnapshotResponseBuilder{builder=" + builder + "}";
        }

        return "GrpcInstallSnapshotResponse{" + "groupId=" + getGroupId() + ", sender=" + sender.getId() + ", term=" + getTerm()
                + ", snapshotIndex=" + getSnapshotIndex() + ", requestedSnapshotChunkIndex=" + getRequestedSnapshotChunkIndex()
                + ", querySeqNo=" + getQuerySeqNo() + ", flowControlSeqNo=" + getFlowControlSeqNo() + '}';
    }

}
