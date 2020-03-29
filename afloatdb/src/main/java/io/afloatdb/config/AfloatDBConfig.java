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

package io.afloatdb.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.afloatdb.AfloatDBException;
import io.microraft.RaftConfig;
import io.microraft.RaftConfig.RaftConfigBuilder;

import javax.annotation.Nonnull;

import static io.microraft.RaftConfig.DEFAULT_RAFT_CONFIG;
import static java.util.Objects.requireNonNull;

public final class AfloatDBConfig {

    private Config config;
    private AfloatDBEndpointConfig localEndpointConfig;
    private RaftGroupConfig raftGroupConfig;
    private RaftConfig raftConfig;

    private AfloatDBConfig() {
    }

    @Nonnull
    public static AfloatDBConfig from(@Nonnull Config config) {
        requireNonNull(config);
        return newBuilder().setConfig(config).build();
    }

    @Nonnull
    public static AfloatDBConfigBuilder newBuilder() {
        return new AfloatDBConfigBuilder();
    }

    private static RaftConfig parseRaftConfig(@Nonnull Config config) {
        if (!config.hasPath("raft")) {
            return DEFAULT_RAFT_CONFIG;
        }

        config = config.getConfig("raft");
        RaftConfigBuilder builder = RaftConfig.newBuilder();

        if (config.hasPath("leader-election-timeout-millis")) {
            builder.setLeaderElectionTimeoutMillis(config.getLong("leader-election-timeout-millis"));
        }

        if (config.hasPath("leader-heartbeat-period-secs")) {
            builder.setLeaderHeartbeatPeriodSecs(config.getLong("leader-heartbeat-period-secs"));
        }

        if (config.hasPath("leader-heartbeat-timeout-secs")) {
            builder.setLeaderHeartbeatTimeoutSecs(config.getLong("leader-heartbeat-timeout-secs"));
        }

        if (config.hasPath("append-entries-request-batch-size")) {
            builder.setAppendEntriesRequestBatchSize(config.getInt("append-entries-request-batch-size"));
        }

        if (config.hasPath("commit-count-to-take-snapshot")) {
            builder.setCommitCountToTakeSnapshot(config.getInt("commit-count-to-take-snapshot"));
        }

        if (config.hasPath("max-uncommitted-log-entry-count")) {
            builder.setMaxUncommittedLogEntryCount(config.getInt("max-uncommitted-log-entry-count"));
        }

        if (config.hasPath("transfer-snapshots-from-followers-enabled")) {
            builder.setTransferSnapshotsFromFollowersEnabled(config.getBoolean("transfer-snapshots-from-followers-enabled"));
        }

        if (config.hasPath("raft-node-report-publish-period-secs")) {
            builder.setRaftNodeReportPublishPeriodSecs(config.getInt("raft-node-report-publish-period-secs"));
        }

        return builder.build();
    }

    @Nonnull
    public Config getConfig() {
        return config;
    }

    @Nonnull
    public AfloatDBEndpointConfig getLocalEndpointConfig() {
        return localEndpointConfig;
    }

    @Nonnull
    public RaftGroupConfig getRaftGroupConfig() {
        return raftGroupConfig;
    }

    @Nonnull
    public RaftConfig getRaftConfig() {
        return raftConfig;
    }

    public static class AfloatDBConfigBuilder {

        private AfloatDBConfig afloatDBConfig = new AfloatDBConfig();

        @Nonnull
        public AfloatDBConfigBuilder setConfig(@Nonnull Config config) {
            requireNonNull(config);
            afloatDBConfig.config = config;
            return this;
        }

        @Nonnull
        public AfloatDBConfigBuilder setLocalEndpointConfig(@Nonnull AfloatDBEndpointConfig localEndpointConfig) {
            requireNonNull(localEndpointConfig);
            afloatDBConfig.localEndpointConfig = localEndpointConfig;
            return this;
        }

        @Nonnull
        public AfloatDBConfigBuilder setRaftGroupConfig(@Nonnull RaftGroupConfig raftGroupConfig) {
            requireNonNull(raftGroupConfig);
            afloatDBConfig.raftGroupConfig = raftGroupConfig;
            return this;
        }

        @Nonnull
        public AfloatDBConfigBuilder setRaftConfig(@Nonnull RaftConfig raftConfig) {
            requireNonNull(raftConfig);
            afloatDBConfig.raftConfig = raftConfig;
            return this;
        }

        @Nonnull
        public AfloatDBConfig build() {
            if (afloatDBConfig == null) {
                throw new AfloatDBException("AfloatDBConfig already built!");
            }

            if (afloatDBConfig.config == null) {
                try {
                    afloatDBConfig.config = ConfigFactory.load();
                } catch (Exception e) {
                    throw new AfloatDBException("Could not load Config!", e);
                }
            }

            try {
                if (afloatDBConfig.config.hasPath("afloatdb")) {
                    Config config = afloatDBConfig.config.getConfig("afloatdb");

                    if (afloatDBConfig.localEndpointConfig == null && config.hasPath("local-endpoint")) {
                        afloatDBConfig.localEndpointConfig = AfloatDBEndpointConfig.from(config.getConfig("local" + "-endpoint"));
                    }

                    if (afloatDBConfig.raftGroupConfig == null && config.hasPath("group")) {
                        afloatDBConfig.raftGroupConfig = RaftGroupConfig.from(config.getConfig("group"));
                    }

                    if (afloatDBConfig.raftConfig == null) {
                        afloatDBConfig.raftConfig = parseRaftConfig(config);
                    }
                }
            } catch (Exception e) {
                if (e instanceof AfloatDBException) {
                    throw (AfloatDBException) e;
                }

                throw new AfloatDBException("Could not build AfloatDBConfig!", e);
            }

            if (afloatDBConfig.localEndpointConfig == null) {
                throw new AfloatDBException("Local endpoint config is missing!");
            }

            if (afloatDBConfig.raftGroupConfig == null) {
                throw new AfloatDBException("Raft group config is missing!");
            }

            if (afloatDBConfig.raftConfig == null) {
                throw new AfloatDBException("Raft config is missing!");
            }

            AfloatDBConfig afloatDBConfig = this.afloatDBConfig;
            this.afloatDBConfig = null;
            return afloatDBConfig;
        }

    }

}
