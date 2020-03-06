package io.afloatdb.internal.raft.impl.model.message;

import io.afloatdb.raft.proto.ProtoRaftMessage;

public interface GrpcRaftMessage {

    void populate(ProtoRaftMessage.Builder builder);

}
