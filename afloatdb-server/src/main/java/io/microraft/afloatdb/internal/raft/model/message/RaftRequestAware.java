package io.microraft.afloatdb.internal.raft.model.message;

import io.microraft.afloatdb.raft.proto.RaftRequest;

public interface RaftRequestAware {

    void populate(RaftRequest.Builder builder);

}
