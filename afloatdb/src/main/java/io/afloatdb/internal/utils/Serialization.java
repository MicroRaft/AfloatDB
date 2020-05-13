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
import io.afloatdb.internal.raft.impl.model.message.AppendEntriesFailureResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.AppendEntriesRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.AppendEntriesSuccessResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.InstallSnapshotRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.InstallSnapshotResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.PreVoteRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.PreVoteResponseOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.RaftMessageProtoAware;
import io.afloatdb.internal.raft.impl.model.message.TriggerLeaderElectionRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.VoteRequestOrBuilder;
import io.afloatdb.internal.raft.impl.model.message.VoteResponseOrBuilder;
import io.afloatdb.management.proto.RaftGroupMembersProto;
import io.afloatdb.management.proto.RaftLogStatsProto;
import io.afloatdb.management.proto.RaftNodeReportProto;
import io.afloatdb.management.proto.RaftNodeReportReasonProto;
import io.afloatdb.management.proto.RaftNodeStatusProto;
import io.afloatdb.management.proto.RaftRoleProto;
import io.afloatdb.management.proto.RaftTermProto;
import io.afloatdb.raft.proto.OperationResponse;
import io.afloatdb.raft.proto.QueryRequest.QUERY_POLICY;
import io.afloatdb.raft.proto.RaftMessageProto;
import io.microraft.QueryPolicy;
import io.microraft.RaftNodeStatus;
import io.microraft.RaftRole;
import io.microraft.model.message.RaftMessage;
import io.microraft.report.RaftGroupMembers;
import io.microraft.report.RaftLogStats;
import io.microraft.report.RaftNodeReport;
import io.microraft.report.RaftNodeReport.RaftNodeReportReason;
import io.microraft.report.RaftTerm;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

public final class Serialization {

    private Serialization() {
    }

    public static RaftNodeReportProto toProto(RaftNodeReport report) {
        return RaftNodeReportProto.newBuilder().setReason(toProto(report.getReason())).setGroupId((String) report.getGroupId())
                                  .setEndpoint(AfloatDBEndpoint.extract(report.getEndpoint()))
                                  .setInitialMembers(toProto(report.getInitialMembers()))
                                  .setCommittedMembers(toProto(report.getCommittedMembers()))
                                  .setEffectiveMembers(toProto(report.getEffectiveMembers())).setRole(toProto(report.getRole()))
                                  .setStatus(toProto(report.getStatus())).setTerm(toProto(report.getTerm()))
                                  .setLog(toProto(report.getLog())).build();
    }

    public static RaftNodeReportReasonProto toProto(RaftNodeReportReason reason) {
        switch (reason) {
            case STATUS_CHANGE:
                return RaftNodeReportReasonProto.STATUS_CHANGE;
            case ROLE_CHANGE:
                return RaftNodeReportReasonProto.ROLE_CHANGE;
            case GROUP_MEMBERS_CHANGE:
                return RaftNodeReportReasonProto.GROUP_MEMBERS_CHANGE;
            case TAKE_SNAPSHOT:
                return RaftNodeReportReasonProto.TAKE_SNAPSHOT;
            case INSTALL_SNAPSHOT:
                return RaftNodeReportReasonProto.INSTALL_SNAPSHOT;
            case PERIODIC:
                return RaftNodeReportReasonProto.PERIODIC;
            case API_CALL:
                return RaftNodeReportReasonProto.API_CALL;
            default:
                throw new IllegalArgumentException("Invalid RaftNodeReportReason: " + reason);
        }
    }

    public static RaftGroupMembersProto toProto(RaftGroupMembers groupMembers) {
        RaftGroupMembersProto.Builder builder = RaftGroupMembersProto.newBuilder();
        builder.setLogIndex(groupMembers.getLogIndex());

        groupMembers.getMembers().stream().map(AfloatDBEndpoint::extract).forEach(builder::addMember);

        return builder.build();
    }

    public static RaftRoleProto toProto(RaftRole role) {
        switch (role) {
            case FOLLOWER:
                return RaftRoleProto.FOLLOWER;
            case CANDIDATE:
                return RaftRoleProto.CANDIDATE;
            case LEADER:
                return RaftRoleProto.LEADER;
            default:
                throw new IllegalArgumentException("Invalid RaftRole: " + role);
        }
    }

    public static RaftNodeStatusProto toProto(RaftNodeStatus status) {
        switch (status) {
            case INITIAL:
                return RaftNodeStatusProto.INITIAL;
            case ACTIVE:
                return RaftNodeStatusProto.ACTIVE;
            case UPDATING_RAFT_GROUP_MEMBER_LIST:
                return RaftNodeStatusProto.UPDATING_RAFT_GROUP_MEMBER_LIST;
            case TERMINATED:
                return RaftNodeStatusProto.TERMINATED;
            default:
                throw new IllegalArgumentException("Invalid RaftNodeStatus: " + status);
        }
    }

    public static RaftTermProto toProto(RaftTerm term) {
        RaftTermProto.Builder builder = RaftTermProto.newBuilder();

        builder.setTerm(term.getTerm());

        if (term.getLeaderEndpoint() != null) {
            builder.setLeaderEndpoint(AfloatDBEndpoint.extract(term.getLeaderEndpoint()));
        }

        if (term.getVotedEndpoint() != null) {
            builder.setVotedEndpoint(AfloatDBEndpoint.extract(term.getVotedEndpoint()));
        }

        return builder.build();
    }

    public static RaftLogStatsProto toProto(RaftLogStats log) {
        return RaftLogStatsProto.newBuilder().setCommitIndex(log.getCommitIndex())
                                .setLastLogOrSnapshotIndex(log.getLastLogOrSnapshotIndex())
                                .setLastLogOrSnapshotTerm(log.getLastLogOrSnapshotTerm())
                                .setSnapshotIndex(log.getLastSnapshotIndex()).setSnapshotTerm(log.getLastSnapshotTerm())
                                .setTakeSnapshotCount(log.getTakeSnapshotCount())
                                .setInstallSnapshotCount(log.getInstallSnapshotCount()).build();
    }

    public static Object unwrapResponse(OperationResponse response) {
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

    public static RaftMessage unwrap(@Nonnull RaftMessageProto proto) {
        switch (proto.getMessageCase()) {
            case VOTEREQUEST:
                return new VoteRequestOrBuilder(proto.getVoteRequest());
            case VOTERESPONSE:
                return new VoteResponseOrBuilder(proto.getVoteResponse());
            case APPENDENTRIESREQUEST:
                return new AppendEntriesRequestOrBuilder(proto.getAppendEntriesRequest());
            case APPENDENTRIESSUCCESSRESPONSE:
                return new AppendEntriesSuccessResponseOrBuilder(proto.getAppendEntriesSuccessResponse());
            case APPENDENTRIESFAILURERESPONSE:
                return new AppendEntriesFailureResponseOrBuilder(proto.getAppendEntriesFailureResponse());
            case INSTALLSNAPSHOTREQUEST:
                return new InstallSnapshotRequestOrBuilder(proto.getInstallSnapshotRequest());
            case INSTALLSNAPSHOTRESPONSE:
                return new InstallSnapshotResponseOrBuilder(proto.getInstallSnapshotResponse());
            case PREVOTEREQUEST:
                return new PreVoteRequestOrBuilder(proto.getPreVoteRequest());
            case PREVOTERESPONSE:
                return new PreVoteResponseOrBuilder(proto.getPreVoteResponse());
            case TRIGGERLEADERELECTIONREQUEST:
                return new TriggerLeaderElectionRequestOrBuilder(proto.getTriggerLeaderElectionRequest());
            default:
                throw new IllegalArgumentException("Invalid proto: " + proto);
        }
    }

    public static RaftMessageProto wrap(@Nonnull RaftMessage message) {
        requireNonNull(message);

        RaftMessageProto.Builder builder = RaftMessageProto.newBuilder();

        if (message instanceof RaftMessageProtoAware) {
            ((RaftMessageProtoAware) message).populate(builder);
        } else {
            throw new IllegalArgumentException("Cannot convert " + message + " to proto");
        }

        return builder.build();
    }
}
