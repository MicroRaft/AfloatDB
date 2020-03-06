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

package io.afloatdb.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.afloatdb.AfloatDBException;
import io.afloatdb.config.AfloatDBConfig.AfloatDBConfigBuilder;
import io.microraft.RaftConfig;
import io.microraft.impl.util.BaseTest;
import org.junit.Test;

import java.util.List;

import static io.microraft.RaftConfig.DEFAULT_RAFT_CONFIG;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class AfloatDBConfigTest
        extends BaseTest {

    @Test(expected = AfloatDBException.class)
    public void when_emptyConfigStringProvided_then_shouldNotCreateConfig() {
        Config config = ConfigFactory.parseString("");

        AfloatDBConfig.from(config);
    }

    @Test(expected = AfloatDBException.class)
    public void when_localEndpointMissingInConfig_then_shouldNotCreateConfig() {
        Config config = ConfigFactory.parseString(
                "afloatdb: {\n" + "\n" + "    group: {\n" + "\n" + "        id: \"kvStore\"\n" + "\n"
                        + "        initial-endpoints: [\n" + "            {id: \"node1\", address: \"localhost:6701\"}," + "\n"
                        + "            {id: \"node2\", address: \"localhost:6702\"}," + "\n"
                        + "            {id: \"node3\", address: \"localhost:6703\"}\n" + "        ]\n" + "\n" + "    }\n" + "\n"
                        + "    raft: {\n" + "        leader-election-timeout-millis: 2000\n"
                        + "        leader-heartbeat-period-millis: 5000\n" + "        leader-heartbeat-timeout-millis: 15000\n"
                        + "        append-entries-request-batch-size: 1000\n" + "        commit-count-to-take-snapshot: "
                        + "50000\n" + "        max-uncommitted-log-entry-count: 1000\n"
                        + "        leader-backoff-duration-millis: 200\n" + "        raft-node-report-publish-period-secs: 10\n"
                        + "    }\n" + "\n" + "}\n");

        AfloatDBConfig.from(config);
    }

    @Test(expected = AfloatDBException.class)
    public void when_groupConfigMissingInConfig_then_shouldNotCreateConfig() {
        Config config = ConfigFactory.parseString(
                "afloatdb: {\n" + "\n" + "    local-endpoint: {id: \"node1\", address: " + "\"localhost:6701\"}\n" + "\n"
                        + "    raft: {\n" + "        leader-election-timeout-millis: 2000\n"
                        + "        leader-heartbeat-period-millis: 5000\n" + "        leader-heartbeat-timeout-millis: 15000\n"
                        + "        append-entries-request-batch-size: 1000\n" + "        commit-count-to-take-snapshot: "
                        + "50000\n" + "        max-uncommitted-log-entry-count: 1000\n"
                        + "        leader-backoff-duration-millis: 200\n" + "        raft-node-report-publish-period-secs: 10\n"
                        + "    }\n" + "\n" + "}\n");

        AfloatDBConfig.from(config);
    }

    @Test
    public void when_localEndpointAndGroupConfigPresentInConfig_then_shouldCreateConfig() {
        Config config = ConfigFactory.parseString(
                "afloatdb: {\n" + "\n" + "    local-endpoint: {id: \"node1\", address: " + "\"localhost:6701\"}\n" + "\n"
                        + "    group: {\n" + "\n" + "        id: \"kvStore\"\n" + "\n" + "        initial-endpoints: [\n"
                        + "            {id: \"node1\", address: \"localhost:6701\"}," + "\n"
                        + "            {id: \"node2\", address: \"localhost:6702\"}," + "\n"
                        + "            {id: \"node3\", address: \"localhost:6703\"}\n" + "        ]\n" + "\n" + "    }\n" + "\n"
                        + "}\n");

        AfloatDBConfig afloatDBConfig = AfloatDBConfig.from(config);

        assertThat(afloatDBConfig.getLocalEndpointConfig().getId()).isEqualTo("node1");
        assertThat(afloatDBConfig.getLocalEndpointConfig().getAddress()).isEqualTo("localhost:6701");
        assertThat(afloatDBConfig.getRaftGroupConfig().getId()).isEqualTo("kvStore");
        List<AfloatDBEndpointConfig> initialEndpoints = afloatDBConfig.getRaftGroupConfig().getInitialEndpoints();
        assertThat(initialEndpoints).hasSize(3);
        assertThat(initialEndpoints.get(0).getId()).isEqualTo("node1");
        assertThat(initialEndpoints.get(0).getAddress()).isEqualTo("localhost:6701");
        assertThat(initialEndpoints.get(1).getId()).isEqualTo("node2");
        assertThat(initialEndpoints.get(1).getAddress()).isEqualTo("localhost:6702");
        assertThat(initialEndpoints.get(2).getId()).isEqualTo("node3");
        assertThat(initialEndpoints.get(2).getAddress()).isEqualTo("localhost:6703");
        assertThat(afloatDBConfig.getRaftConfig()).isSameAs(DEFAULT_RAFT_CONFIG);
    }

    @Test
    public void when_raftConfigPresentInConfig_then_shouldCreateConfig() {
        Config config = ConfigFactory.parseString(
                "afloatdb: {\n" + "\n" + "    local-endpoint: {id: \"node1\", address: " + "\"localhost:6701\"}\n" + "\n"
                        + "    group: {\n" + "\n" + "        id: \"kvStore\"\n" + "\n" + "        initial-endpoints: [\n"
                        + "            {id: \"node1\", address: \"localhost:6701\"}," + "\n"
                        + "            {id: \"node2\", address: \"localhost:6702\"}," + "\n"
                        + "            {id: \"node3\", address: \"localhost:6703\"}\n" + "        ]\n" + "\n" + "    }\n" + "\n"
                        + "    raft: {\n" + "        leader-election-timeout-millis: 2500\n"
                        + "        leader-heartbeat-period-millis: 4000\n" + "        leader-heartbeat-timeout-millis: 8000\n"
                        + "        append-entries-request-batch-size: 150\n" + "        commit-count-to-take-snapshot: "
                        + "1000\n" + "        max-uncommitted-log-entry-count: 50\n"
                        + "        leader-backoff-duration-millis: 500\n" + "        raft-node-report-publish-period-secs: 30\n"
                        + "    }\n" + "\n" + "}\n");

        AfloatDBConfig afloatDBConfig = AfloatDBConfig.from(config);

        assertThat(afloatDBConfig.getLocalEndpointConfig().getId()).isEqualTo("node1");
        assertThat(afloatDBConfig.getLocalEndpointConfig().getAddress()).isEqualTo("localhost:6701");
        assertThat(afloatDBConfig.getRaftGroupConfig().getId()).isEqualTo("kvStore");
        List<AfloatDBEndpointConfig> initialEndpoints = afloatDBConfig.getRaftGroupConfig().getInitialEndpoints();
        assertThat(initialEndpoints).hasSize(3);
        assertThat(initialEndpoints.get(0).getId()).isEqualTo("node1");
        assertThat(initialEndpoints.get(0).getAddress()).isEqualTo("localhost:6701");
        assertThat(initialEndpoints.get(1).getId()).isEqualTo("node2");
        assertThat(initialEndpoints.get(1).getAddress()).isEqualTo("localhost:6702");
        assertThat(initialEndpoints.get(2).getId()).isEqualTo("node3");
        assertThat(initialEndpoints.get(2).getAddress()).isEqualTo("localhost:6703");
        assertThat(afloatDBConfig.getRaftConfig()).isNotNull();
        assertThat(afloatDBConfig.getRaftConfig().getLeaderElectionTimeoutMillis()).isEqualTo(2500L);
        assertThat(afloatDBConfig.getRaftConfig().getLeaderHeartbeatPeriodMillis()).isEqualTo(4000L);
        assertThat(afloatDBConfig.getRaftConfig().getLeaderHeartbeatTimeoutMillis()).isEqualTo(8000L);
        assertThat(afloatDBConfig.getRaftConfig().getAppendEntriesRequestBatchSize()).isEqualTo(150);
        assertThat(afloatDBConfig.getRaftConfig().getCommitCountToTakeSnapshot()).isEqualTo(1000);
        assertThat(afloatDBConfig.getRaftConfig().getMaxUncommittedLogEntryCount()).isEqualTo(50);
        assertThat(afloatDBConfig.getRaftConfig().getLeaderBackoffDurationMillis()).isEqualTo(500);
        assertThat(afloatDBConfig.getRaftConfig().getRaftNodeReportPublishPeriodSecs()).isEqualTo(30);
    }

    @Test
    public void when_groupAndRaftConfigPresentInBothConfigAndBuilder_then_shouldCreateConfigWithBuilder() {
        Config config = ConfigFactory.parseString(
                "afloatdb: {\n" + "\n" + "    local-endpoint: {id: \"node1\", address: \"localhost:6701\"}\n" + "\n"
                        + "    group: {\n" + "\n" + "        id: \"kvStore\"\n" + "\n" + "        initial-endpoints: [\n"
                        + "            {id: \"node1\", address: \"localhost:6701\"},\n"
                        + "            {id: \"node2\", address: \"localhost:6702\"},\n"
                        + "            {id: \"node3\", address: \"localhost:6703\"}\n" + "        ]\n" + "\n" + "    }\n" + "\n"
                        + "    raft: {\n" + "        leader-election-timeout-millis: 2500\n" + "    }\n" + "\n" + "}\n");

        AfloatDBConfigBuilder builder = AfloatDBConfig.newBuilder();
        builder.setConfig(config);

        AfloatDBEndpointConfig endpointConfig1 = AfloatDBEndpointConfig.newBuilder().setId("node1").setAddress("localhost:6767")
                                                                       .build();
        AfloatDBEndpointConfig endpointConfig2 = AfloatDBEndpointConfig.newBuilder().setId("node2").setAddress("localhost:6768")
                                                                       .build();
        AfloatDBEndpointConfig endpointConfig3 = AfloatDBEndpointConfig.newBuilder().setId("node3").setAddress("localhost:6769")
                                                                       .build();
        RaftGroupConfig groupConfig = RaftGroupConfig.newBuilder().setId("group1").setInitialEndpoints(
                asList(endpointConfig1, endpointConfig2, endpointConfig3)).build();
        RaftConfig raftConfig = RaftConfig.newBuilder().setLeaderElectionTimeoutMillis(2500L).build();

        AfloatDBConfig afloatDBConfig = builder.setLocalEndpointConfig(endpointConfig1).setRaftGroupConfig(groupConfig)
                                               .setRaftConfig(raftConfig).build();

        assertThat(afloatDBConfig.getConfig()).isSameAs(config);
        assertThat(afloatDBConfig.getLocalEndpointConfig()).isSameAs(endpointConfig1);
        assertThat(afloatDBConfig.getRaftGroupConfig()).isSameAs(groupConfig);
        assertThat(afloatDBConfig.getRaftConfig()).isSameAs(raftConfig);
    }

}
