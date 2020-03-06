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

package io.afloatdb.internal.rpc.impl;

import io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint;
import io.afloatdb.management.proto.ProtoRaftGroupMembers;
import io.afloatdb.management.proto.ProtoRaftGroupTerm;
import io.afloatdb.management.proto.ProtoRaftLogStats;
import io.afloatdb.management.proto.ProtoRaftNodeReport;
import io.afloatdb.management.proto.ProtoRaftNodeReportReason;
import io.afloatdb.management.proto.ProtoRaftNodeStatus;
import io.afloatdb.management.proto.ProtoRaftRole;
import io.microraft.RaftNodeStatus;
import io.microraft.RaftRole;
import io.microraft.report.RaftGroupMembers;
import io.microraft.report.RaftGroupTerm;
import io.microraft.report.RaftLogStats;
import io.microraft.report.RaftNodeReport;
import io.microraft.report.RaftNodeReport.RaftNodeReportReason;

import static io.afloatdb.internal.raft.impl.model.AfloatDBEndpoint.extract;

public final class SerializationUtils {

    private SerializationUtils() {
    }

    public static ProtoRaftNodeReport toProto(RaftNodeReport report) {
        return ProtoRaftNodeReport.newBuilder().setReason(toProto(report.getReason())).setGroupId((String) report.getGroupId())
                                  .setEndpoint(extract(report.getEndpoint()))
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
            builder.setLeaderEndpoint(extract(term.getLeaderEndpoint()));
        }

        if (term.getVotedEndpoint() != null) {
            builder.setVotedEndpoint(extract(term.getVotedEndpoint()));
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

}
