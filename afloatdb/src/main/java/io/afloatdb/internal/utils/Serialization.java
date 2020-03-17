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

package io.afloatdb.internal.utils;

import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
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
import io.afloatdb.management.proto.ProtoRaftGroupMembers;
import io.afloatdb.management.proto.ProtoRaftGroupTerm;
import io.afloatdb.management.proto.ProtoRaftLogStats;
import io.afloatdb.management.proto.ProtoRaftNodeReport;
import io.afloatdb.management.proto.ProtoRaftNodeReportReason;
import io.afloatdb.management.proto.ProtoRaftNodeStatus;
import io.afloatdb.management.proto.ProtoRaftRole;
import io.afloatdb.raft.proto.ProtoOperationResponse;
import io.afloatdb.raft.proto.ProtoQueryRequest.QUERY_POLICY;
import io.afloatdb.raft.proto.ProtoRaftMessage;
import io.microraft.QueryPolicy;
import io.microraft.RaftNodeStatus;
import io.microraft.RaftRole;
import io.microraft.model.message.RaftMessage;
import io.microraft.report.RaftGroupMembers;
import io.microraft.report.RaftGroupTerm;
import io.microraft.report.RaftLogStats;
import io.microraft.report.RaftNodeReport;
import io.microraft.report.RaftNodeReport.RaftNodeReportReason;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

public final class Serialization {

    private Serialization() {
    }

    public static ProtoRaftNodeReport toProto(RaftNodeReport report) {
        return ProtoRaftNodeReport.newBuilder().setReason(toProto(report.getReason())).setGroupId((String) report.getGroupId())
                                  .setEndpoint(AfloatDBEndpoint.extract(report.getEndpoint()))
                                  .setInitialMembers(toProto(report.getInitialMembers()))
                                  .setCommittedMembers(toProto(report.getCommittedMembers()))
                                  .setEffectiveMembers(toProto(report.getEffectiveMembers())).setRole(toProto(report.getRole()))
                                  .setStatus(toProto(report.getStatus())).setTerm(toProto(report.getTerm()))
                                  .setLog(toProto(report.getLog())).build();
    }

    public static ProtoRaftNodeReportReason toProto(RaftNodeReportReason reason) {
        switch (reason) {
            case STATUS_CHANGE:
                return ProtoRaftNodeReportReason.STATUS_CHANGE;
            case ROLE_CHANGE:
                return ProtoRaftNodeReportReason.ROLE_CHANGE;
            case GROUP_MEMBERS_CHANGE:
                return ProtoRaftNodeReportReason.GROUP_MEMBERS_CHANGE;
            case TAKE_SNAPSHOT:
                return ProtoRaftNodeReportReason.TAKE_SNAPSHOT;
            case INSTALL_SNAPSHOT:
                return ProtoRaftNodeReportReason.INSTALL_SNAPSHOT;
            case PERIODIC:
                return ProtoRaftNodeReportReason.PERIODIC;
            case API_CALL:
                return ProtoRaftNodeReportReason.API_CALL;
            default:
                throw new IllegalArgumentException("Invalid RaftNodeReportReason: " + reason);
        }
    }

    public static ProtoRaftGroupMembers toProto(RaftGroupMembers groupMembers) {
        ProtoRaftGroupMembers.Builder builder = ProtoRaftGroupMembers.newBuilder();
        builder.setLogIndex(groupMembers.getLogIndex());

        groupMembers.getMembers().stream().map(AfloatDBEndpoint::extract).forEach(builder::addMember);

        return builder.build();
    }

    public static ProtoRaftRole toProto(RaftRole role) {
        switch (role) {
            case FOLLOWER:
                return ProtoRaftRole.FOLLOWER;
            case CANDIDATE:
                return ProtoRaftRole.CANDIDATE;
            case LEADER:
                return ProtoRaftRole.LEADER;
            default:
                throw new IllegalArgumentException("Invalid RaftRole: " + role);
        }
    }

    public static ProtoRaftNodeStatus toProto(RaftNodeStatus status) {
        switch (status) {
            case INITIAL:
                return ProtoRaftNodeStatus.INITIAL;
            case ACTIVE:
                return ProtoRaftNodeStatus.ACTIVE;
            case UPDATING_RAFT_GROUP_MEMBER_LIST:
                return ProtoRaftNodeStatus.UPDATING_RAFT_GROUP_MEMBER_LIST;
            case TERMINATING_RAFT_GROUP:
                return ProtoRaftNodeStatus.TERMINATING_RAFT_GROUP;
            case TERMINATED:
                return ProtoRaftNodeStatus.TERMINATED;
            default:
                throw new IllegalArgumentException("Invalid RaftNodeStatus: " + status);
        }
    }

    public static ProtoRaftGroupTerm toProto(RaftGroupTerm term) {
        ProtoRaftGroupTerm.Builder builder = ProtoRaftGroupTerm.newBuilder();

        builder.setTerm(term.getTerm());

        if (term.getLeaderEndpoint() != null) {
            builder.setLeaderEndpoint(AfloatDBEndpoint.extract(term.getLeaderEndpoint()));
        }

        if (term.getVotedEndpoint() != null) {
            builder.setVotedEndpoint(AfloatDBEndpoint.extract(term.getVotedEndpoint()));
        }

        return builder.build();
    }

    public static ProtoRaftLogStats toProto(RaftLogStats log) {
        return ProtoRaftLogStats.newBuilder().setCommitIndex(log.getCommitIndex())
                                .setLastLogOrSnapshotIndex(log.getLastLogOrSnapshotIndex())
                                .setLastLogOrSnapshotTerm(log.getLastLogOrSnapshotTerm())
                                .setSnapshotIndex(log.getLastSnapshotIndex()).setSnapshotTerm(log.getLastSnapshotTerm())
                                .setTakeSnapshotCount(log.getTakeSnapshotCount())
                                .setInstallSnapshotCount(log.getInstallSnapshotCount()).build();
    }

    public static Object unwrapResponse(ProtoOperationResponse response) {
        switch (response.getResponseCase()) {
            case PUTRESPONSE:
                return response.getPutResponse();
            case SETRESPONSE:
                return response.getSetResponse();
            case GETRESPONSE:
                return response.getGetResponse();
            case CONTAINSRESPONSE:
                return response.getContainsResponse();
            case DELETERESPONSE:
                return response.getDeleteResponse();
            case REMOVERESPONSE:
                return response.getRemoveResponse();
            case REPLACERESPONSE:
                return response.getReplaceResponse();
            case SIZERESPONSE:
                return response.getSizeResponse();
            case CLEARRESPONSE:
                return response.getClearResponse();
            default:
                throw new IllegalArgumentException("Invalid response: " + response);
        }
    }

    public static QUERY_POLICY toProto(@Nonnull QueryPolicy queryPolicy) {
        switch (queryPolicy) {
            case ANY_LOCAL:
                return QUERY_POLICY.ANY_LOCAL;
            case LEADER_LOCAL:
                return QUERY_POLICY.LEADER_LOCAL;
            case LINEARIZABLE:
                return QUERY_POLICY.LINEARIZABLE;
            default:
                throw new IllegalArgumentException("Invalid query policy: " + queryPolicy);
        }
    }

    public static QueryPolicy fromProto(QUERY_POLICY queryPolicy) {
        switch (queryPolicy.getNumber()) {
            case QUERY_POLICY.ANY_LOCAL_VALUE:
                return QueryPolicy.ANY_LOCAL;
            case QUERY_POLICY.LEADER_LOCAL_VALUE:
                return QueryPolicy.LEADER_LOCAL;
            case QUERY_POLICY.LINEARIZABLE_VALUE:
                return QueryPolicy.LINEARIZABLE;
            default:
                return null;
        }
    }

    public static RaftMessage unwrap(@Nonnull ProtoRaftMessage proto) {
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
