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

package io.afloatdb.internal.raft.impl.model;

import io.afloatdb.internal.raft.impl.model.groupop.GrpcUpdateRaftGroupMembersOpOrBuilder;
import io.afloatdb.internal.raft.impl.model.log.GrpcLogEntryOrBuilder;
import io.afloatdb.internal.raft.impl.model.log.GrpcSnapshotChunkOrBuilder;
import io.afloatdb.internal.raft.impl.model.log.GrpcSnapshotEntryOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcAppendEntriesFailureResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcAppendEntriesRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcAppendEntriesSuccessResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcInstallSnapshotRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcInstallSnapshotResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcPreVoteRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcPreVoteResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcTriggerLeaderElectionRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcVoteRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.GrpcVoteResponseOrBuilder;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp.UpdateRaftGroupMembersOpBuilder;
import io.microraft.model.log.LogEntry.LogEntryBuilder;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.log.SnapshotEntry.SnapshotEntryBuilder;
import io.microraft.model.message.AppendEntriesFailureResponse.AppendEntriesFailureResponseBuilder;
import io.microraft.model.message.AppendEntriesRequest.AppendEntriesRequestBuilder;
import io.microraft.model.message.AppendEntriesSuccessResponse.AppendEntriesSuccessResponseBuilder;
import io.microraft.model.message.InstallSnapshotRequest.InstallSnapshotRequestBuilder;
import io.microraft.model.message.InstallSnapshotResponse.InstallSnapshotResponseBuilder;
import io.microraft.model.message.PreVoteRequest.PreVoteRequestBuilder;
import io.microraft.model.message.PreVoteResponse.PreVoteResponseBuilder;
import io.microraft.model.message.TriggerLeaderElectionRequest.TriggerLeaderElectionRequestBuilder;
import io.microraft.model.message.VoteRequest.VoteRequestBuilder;
import io.microraft.model.message.VoteResponse.VoteResponseBuilder;

import javax.annotation.Nonnull;
import javax.inject.Singleton;

@Singleton
public class GrpcRaftModelFactory
        implements RaftModelFactory {

    @Nonnull
    @Override
    public LogEntryBuilder createLogEntryBuilder() {
        return new GrpcLogEntryOrBuilder();
    }

    @Nonnull
    @Override
    public SnapshotEntryBuilder createSnapshotEntryBuilder() {
        return new GrpcSnapshotEntryOrBuilder();
    }

    @Nonnull
    @Override
    public SnapshotChunk.SnapshotChunkBuilder createSnapshotChunkBuilder() {
        return new GrpcSnapshotChunkOrBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesRequestBuilder createAppendEntriesRequestBuilder() {
        return new GrpcAppendEntriesRequestOrBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesSuccessResponseBuilder createAppendEntriesSuccessResponseBuilder() {
        return new GrpcAppendEntriesSuccessResponseOrBuilder();
    }

    @Nonnull
    @Override
    public AppendEntriesFailureResponseBuilder createAppendEntriesFailureResponseBuilder() {
        return new GrpcAppendEntriesFailureResponseOrBuilder();
    }

    @Nonnull
    @Override
    public InstallSnapshotRequestBuilder createInstallSnapshotRequestBuilder() {
        return new GrpcInstallSnapshotRequestOrBuilder();
    }

    @Nonnull
    @Override
    public InstallSnapshotResponseBuilder createInstallSnapshotResponseBuilder() {
        return new GrpcInstallSnapshotResponseOrBuilder();
    }

    @Nonnull
    @Override
    public PreVoteRequestBuilder createPreVoteRequestBuilder() {
        return new GrpcPreVoteRequestOrBuilder();
    }

    @Nonnull
    @Override
    public PreVoteResponseBuilder createPreVoteResponseBuilder() {
        return new GrpcPreVoteResponseOrBuilder();
    }

    @Nonnull
    @Override
    public TriggerLeaderElectionRequestBuilder createTriggerLeaderElectionRequestBuilder() {
        return new GrpcTriggerLeaderElectionRequestOrBuilder();
    }

    @Nonnull
    @Override
    public VoteRequestBuilder createVoteRequestBuilder() {
        return new GrpcVoteRequestOrBuilder();
    }

    @Nonnull
    @Override
    public VoteResponseBuilder createVoteResponseBuilder() {
        return new GrpcVoteResponseOrBuilder();
    }

    @Nonnull
    @Override
    public UpdateRaftGroupMembersOpBuilder createUpdateRaftGroupMembersOpBuilder() {
        return new GrpcUpdateRaftGroupMembersOpOrBuilder();
    }

}
