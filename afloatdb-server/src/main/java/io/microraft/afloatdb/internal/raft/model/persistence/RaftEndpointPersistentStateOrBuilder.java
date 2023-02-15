package io.microraft.afloatdb.internal.raft.model.persistence;

import io.microraft.RaftEndpoint;
import io.microraft.model.persistence.RaftEndpointPersistentState;
import io.microraft.afloatdb.raft.proto.RaftEndpointPersistentStateProto;
import io.microraft.afloatdb.raft.proto.RaftRequest.Builder;
import io.microraft.afloatdb.internal.raft.model.AfloatDBEndpoint;

public class RaftEndpointPersistentStateOrBuilder
        implements
            RaftEndpointPersistentState,
            RaftEndpointPersistentState.RaftEndpointPersistentStateBuilder {

    private RaftEndpointPersistentStateProto.Builder builder;
    private RaftEndpointPersistentStateProto state;
    private RaftEndpoint localEndpoint;

    public RaftEndpointPersistentStateOrBuilder() {
        this.builder = RaftEndpointPersistentStateProto.newBuilder();
    }

    public RaftEndpointPersistentStateProto getState() {
        return state;
    }

    @Override
    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    @Override
    public boolean isVoting() {
        return state.getVoting();
    }

    @Override
    public RaftEndpointPersistentStateBuilder setLocalEndpoint(RaftEndpoint localEndpoint) {
        builder.setLocalEndpoint(AfloatDBEndpoint.unwrap(localEndpoint));
        this.localEndpoint = localEndpoint;
        return this;
    }

    @Override
    public RaftEndpointPersistentStateBuilder setVoting(boolean voting) {
        builder.setVoting(voting);
        return this;
    }

    @Override
    public RaftEndpointPersistentState build() {
        state = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "RaftEndpointPersistentState{builder=" + builder + "}";
        }

        return "RaftEndpointPersistentState{localEndpoint=" + localEndpoint.getId() + ", voting=" + isVoting() + '}';
    }

}
