package io.afloatdb.internal.raft.impl.model.message;

import io.afloatdb.raft.proto.RaftMessageProto;

public interface RaftMessageProtoAware {

    void populate(RaftMessageProto.Builder builder);

}
