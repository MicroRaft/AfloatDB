/*
 * Copyright (c) 2020, MicroRaft.
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

package io.afloatdb.internal.raft.impl;

import io.afloatdb.internal.raft.impl.model.message.GrpcAppendEntriesFailureResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcAppendEntriesRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcAppendEntriesSuccessResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcInstallSnapshotRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcInstallSnapshotResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcPreVoteRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcPreVoteResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcRaftMessage;
import io.afloatdb.internal.raft.impl.model.message.GrpcTriggerLeaderElectionRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcVoteRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcVoteResponseOrBuilder;
import io.afloatdb.raft.proto.ProtoRaftMessage;
import io.microraft.model.message.RaftMessage;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

public final class RaftMessages {

    private RaftMessages() {
    }

    public static RaftMessage extract(@Nonnull ProtoRaftMessage proto) {
        requireNonNull(proto);

        switch (proto.getMessageCase()) {
            case VOTEREQUEST:
                return new GrpcVoteRequestOrBuilder(proto.getVoteRequest());
            case VOTERESPONSE:
                return new GrpcVoteResponseOrBuilder(proto.getVoteResponse());
            case APPENDENTRIESREQUEST:
                return new GrpcAppendEntriesRequestOrBuilder(proto.getAppendEntriesRequest());
            case APPENDENTRIESSUCCESSRESPONSE:
                return new GrpcAppendEntriesSuccessResponseOrBuilder(proto.getAppendEntriesSuccessResponse());
            case APPENDENTRIESFAILURERESPONSE:
                return new GrpcAppendEntriesFailureResponseOrBuilder(proto.getAppendEntriesFailureResponse());
            case INSTALLSNAPSHOTREQUEST:
                return new GrpcInstallSnapshotRequestOrBuilder(proto.getInstallSnapshotRequest());
            case INSTALLSNAPSHOTRESPONSE:
                return new GrpcInstallSnapshotResponseOrBuilder(proto.getInstallSnapshotResponse());
            case PREVOTEREQUEST:
                return new GrpcPreVoteRequestOrBuilder(proto.getPreVoteRequest());
            case PREVOTERESPONSE:
                return new GrpcPreVoteResponseOrBuilder(proto.getPreVoteResponse());
            case TRIGGERLEADERELECTIONREQUEST:
                return new GrpcTriggerLeaderElectionRequestOrBuilder(proto.getTriggerLeaderElectionRequest());
            default:
                throw new IllegalArgumentException("Invalid proto msg: " + proto);
        }
    }

    public static ProtoRaftMessage wrap(@Nonnull RaftMessage message) {
        requireNonNull(message);

        ProtoRaftMessage.Builder builder = ProtoRaftMessage.newBuilder();

        if (message instanceof GrpcRaftMessage) {
            ((GrpcRaftMessage) message).populate(builder);
        } else {
            throw new IllegalArgumentException("Cannot convert " + message + " to proto");
        }

        return builder.build();
    }

}
