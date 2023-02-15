package io.microraft.afloatdb.internal.raft.model.persistence;

import javax.annotation.Nullable;

import io.microraft.RaftEndpoint;
import io.microraft.model.persistence.RaftTermPersistentState;
import io.microraft.afloatdb.raft.proto.RaftRequest.Builder;
import io.microraft.afloatdb.raft.proto.RaftTermPersistentStateProto;
import io.microraft.afloatdb.internal.raft.model.AfloatDBEndpoint;

public class RaftTermPersistentStateOrBuilder
        implements
            RaftTermPersistentState,
            RaftTermPersistentState.RaftTermPersistentStateBuilder {

    private RaftTermPersistentStateProto.Builder builder;
    private RaftTermPersistentStateProto state;
    private RaftEndpoint votedFor;

    public RaftTermPersistentStateOrBuilder() {
        this.builder = RaftTermPersistentStateProto.newBuilder();
    }

    public RaftTermPersistentStateProto getState() {
        return state;
    }

    @Override
    public int getTerm() {
        return state.getTerm();
    }

    @Override
    public RaftEndpoint getVotedFor() {
        return votedFor;
    }

    @Override
    public RaftTermPersistentStateBuilder setTerm(int term) {
        builder.setTerm(term);
        return this;
    }

    @Override
    public RaftTermPersistentStateBuilder setVotedFor(@Nullable RaftEndpoint votedFor) {
        if (votedFor != null) {
            builder.setVotedFor(AfloatDBEndpoint.unwrap(votedFor));
            this.votedFor = votedFor;
        }
        return this;
    }

    @Override
    public RaftTermPersistentState build() {
        state = builder.build();
        builder = null;
        return this;
    }

    @Override
    public String toString() {
        if (builder != null) {
            return "RaftTermPersistentState{builder=" + builder + "}";
        }

        return "RaftTermPersistentState{term=" + state.getTerm() + ", votedFor=" + votedFor.getId() + '}';
    }

}
