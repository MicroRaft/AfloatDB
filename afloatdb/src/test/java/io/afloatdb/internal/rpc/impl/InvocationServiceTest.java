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

package io.afloatdb.internal.rpc.impl;

import com.google.protobuf.ByteString;
import io.afloatdb.AfloatDB;
import io.afloatdb.internal.invocation.InvocationService;
import io.afloatdb.kv.proto.KVResponse;
import io.afloatdb.kv.proto.PutRequest;
import io.afloatdb.kv.proto.SizeRequest;
import io.afloatdb.kv.proto.TypedValue;
import io.afloatdb.raft.proto.Operation;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.microraft.Ordered;
import io.microraft.test.util.BaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.afloatdb.utils.AfloatDBTestUtils.CONFIG_1;
import static io.afloatdb.utils.AfloatDBTestUtils.CONFIG_2;
import static io.afloatdb.utils.AfloatDBTestUtils.CONFIG_3;
import static io.afloatdb.utils.AfloatDBTestUtils.getAnyFollower;
import static io.afloatdb.utils.AfloatDBTestUtils.getFollowers;
import static io.afloatdb.utils.AfloatDBTestUtils.getInvocationService;
import static io.afloatdb.utils.AfloatDBTestUtils.waitUntilLeaderElected;
import static io.microraft.QueryPolicy.EVENTUAL_CONSISTENCY;
import static io.microraft.QueryPolicy.LEADER_LEASE;
import static io.microraft.QueryPolicy.LINEARIZABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class InvocationServiceTest extends BaseTest {

    private final List<AfloatDB> servers = new ArrayList<>();

    @Before
    public void init() {
        servers.add(AfloatDB.bootstrap(CONFIG_1));
        servers.add(AfloatDB.bootstrap(CONFIG_2));
        servers.add(AfloatDB.bootstrap(CONFIG_3));
    }

    @After
    public void tearDown() {
        servers.forEach(AfloatDB::shutdown);
    }

    @Test
    public void when_invocationIsDoneOnLeader_then_invocationSucceeds() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        CompletableFuture<Ordered<KVResponse>> future = invokePutRequest(leader);

        assertThat(future.join().getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void when_invocationIsDoneOnFollower_then_invocationSucceeds() {
        waitUntilLeaderElected(servers);
        AfloatDB follower = getAnyFollower(servers);

        CompletableFuture<Ordered<KVResponse>> future = invokePutRequest(follower);

        assertThat(future.join().getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void when_invocationIsDoneOnMinorityFollower_then_invocationEventuallyFails() {
        waitUntilLeaderElected(servers);
        AfloatDB follower = getAnyFollower(servers);
        for (AfloatDB server : servers) {
            if (server != follower) {
                server.shutdown();
            }
        }

        CompletableFuture<Ordered<KVResponse>> future = invokePutRequest(follower);

        try {
            future.join();
            fail("Invocation should not succeed when the majority is lost!");
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(StatusRuntimeException.class);
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertThat(cause.getStatus().getCode()).isEqualTo(Status.UNAVAILABLE.getCode());
        }
    }

    @Test
    public void when_leaderDemotesToFollowerDuringInvocation_then_invocationFails() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        for (AfloatDB server : getFollowers(servers)) {
            server.shutdown();
        }

        CompletableFuture<Ordered<KVResponse>> future = invokePutRequest(leader);

        try {
            future.join();
            fail("Invocation should not succeed when the majority is lost!");
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(StatusRuntimeException.class);
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertThat(cause.getStatus().getCode()).isEqualTo(Status.DEADLINE_EXCEEDED.getCode());
        }
    }

    @Test
    public void testLinearizableQueryFromFollower() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        invokePutRequest(leader).join();

        AfloatDB follower = getAnyFollower(servers);

        InvocationService invocationService = getInvocationService(follower);
        Operation request = Operation.newBuilder().setSizeRequest(SizeRequest.getDefaultInstance()).build();
        CompletableFuture<Ordered<KVResponse>> future = invocationService.query(request, LINEARIZABLE, 0);

        Ordered<KVResponse> result = future.join();
        assertThat(result.getResult().getSizeResponse().getSize()).isEqualTo(1);
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testLinearizableQueryFromLeader() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        invokePutRequest(leader).join();

        InvocationService invocationService = getInvocationService(leader);
        Operation request = Operation.newBuilder().setSizeRequest(SizeRequest.getDefaultInstance()).build();
        CompletableFuture<Ordered<KVResponse>> future = invocationService.query(request, LINEARIZABLE, 0);

        Ordered<KVResponse> result = future.join();
        assertThat(result.getResult().getSizeResponse().getSize()).isEqualTo(1);
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testLeaderLocalQueryFromFollower() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        invokePutRequest(leader).join();

        AfloatDB follower = getAnyFollower(servers);

        InvocationService invocationService = getInvocationService(follower);
        Operation request = Operation.newBuilder().setSizeRequest(SizeRequest.getDefaultInstance()).build();
        CompletableFuture<Ordered<KVResponse>> future = invocationService.query(request, LEADER_LEASE, 0);

        Ordered<KVResponse> result = future.join();
        assertThat(result.getResult().getSizeResponse().getSize()).isEqualTo(1);
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testLeaderLocalQueryFromLeader() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        invokePutRequest(leader).join();

        InvocationService invocationService = getInvocationService(leader);
        Operation request = Operation.newBuilder().setSizeRequest(SizeRequest.getDefaultInstance()).build();
        CompletableFuture<Ordered<KVResponse>> future = invocationService.query(request, LEADER_LEASE, 0);

        Ordered<KVResponse> result = future.join();
        assertThat(result.getResult().getSizeResponse().getSize()).isEqualTo(1);
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testAnyLocalQueryFromFollower() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        invokePutRequest(leader).join();

        AfloatDB follower = getAnyFollower(servers);

        InvocationService invocationService = getInvocationService(follower);
        Operation request = Operation.newBuilder().setSizeRequest(SizeRequest.getDefaultInstance()).build();
        CompletableFuture<Ordered<KVResponse>> future = invocationService.query(request, EVENTUAL_CONSISTENCY, 0);

        Ordered<KVResponse> result = future.join();
        assertThat(result.getResult().getSizeResponse().getSize()).isLessThanOrEqualTo(1);
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testAnyLocalQueryFromLeader() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        invokePutRequest(leader).join();

        InvocationService invocationService = getInvocationService(leader);
        Operation request = Operation.newBuilder().setSizeRequest(SizeRequest.getDefaultInstance()).build();
        CompletableFuture<Ordered<KVResponse>> future = invocationService.query(request, EVENTUAL_CONSISTENCY, 0);

        Ordered<KVResponse> result = future.join();
        assertThat(result.getResult().getSizeResponse().getSize()).isEqualTo(1);
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    private CompletableFuture<Ordered<KVResponse>> invokePutRequest(AfloatDB server) {
        InvocationService invocationService = getInvocationService(server);
        TypedValue val = TypedValue.newBuilder().setValue(ByteString.copyFromUtf8("val")).build();
        PutRequest request = PutRequest.newBuilder().setKey("key").setValue(val).build();
        Operation operation = Operation.newBuilder().setPutRequest(request).build();

        return invocationService.invoke(operation);
    }

}
