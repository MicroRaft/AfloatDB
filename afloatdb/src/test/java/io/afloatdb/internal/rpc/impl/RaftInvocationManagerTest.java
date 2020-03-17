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
import io.afloatdb.internal.invocation.RaftInvocationManager;
import io.afloatdb.kv.proto.PutRequest;
import io.afloatdb.kv.proto.PutResponse;
import io.afloatdb.kv.proto.SizeRequest;
import io.afloatdb.kv.proto.SizeResponse;
import io.afloatdb.kv.proto.TypedValue;
import io.afloatdb.raft.proto.ProtoOperation;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.microraft.Ordered;
import io.microraft.impl.util.BaseTest;
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
import static io.afloatdb.utils.AfloatDBTestUtils.getRaftInvocationManager;
import static io.afloatdb.utils.AfloatDBTestUtils.waitUntilLeaderElected;
import static io.microraft.QueryPolicy.ANY_LOCAL;
import static io.microraft.QueryPolicy.LEADER_LOCAL;
import static io.microraft.QueryPolicy.LINEARIZABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class RaftInvocationManagerTest
        extends BaseTest {

    private List<AfloatDB> servers = new ArrayList<>();

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
        CompletableFuture<Ordered<PutResponse>> future = invokePutRequest(leader);

        Ordered<PutResponse> result = future.join();
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void when_invocationIsDoneOnFollower_then_invocationSucceeds() {
        waitUntilLeaderElected(servers);
        AfloatDB follower = getAnyFollower(servers);

        CompletableFuture<Ordered<PutResponse>> future = invokePutRequest(follower);

        Ordered<PutResponse> result = future.join();
        assertThat(result.getCommitIndex()).isGreaterThan(0);
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

        CompletableFuture<Ordered<PutResponse>> future = invokePutRequest(follower);

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

        CompletableFuture<Ordered<PutResponse>> future = invokePutRequest(leader);

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

        RaftInvocationManager invocationManager = getRaftInvocationManager(follower);
        ProtoOperation request = ProtoOperation.newBuilder().setSizeRequest(SizeRequest.getDefaultInstance()).build();
        CompletableFuture<Ordered<SizeResponse>> future = invocationManager.query(request, LINEARIZABLE, 0);

        Ordered<SizeResponse> result = future.join();
        assertThat(result.getResult().getSize()).isEqualTo(1);
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testLinearizableQueryFromLeader() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        invokePutRequest(leader).join();

        RaftInvocationManager invocationManager = getRaftInvocationManager(leader);
        ProtoOperation request = ProtoOperation.newBuilder().setSizeRequest(SizeRequest.getDefaultInstance()).build();
        CompletableFuture<Ordered<SizeResponse>> future = invocationManager.query(request, LINEARIZABLE, 0);

        Ordered<SizeResponse> result = future.join();
        assertThat(result.getResult().getSize()).isEqualTo(1);
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testLeaderLocalQueryFromFollower() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        invokePutRequest(leader).join();

        AfloatDB follower = getAnyFollower(servers);

        RaftInvocationManager invocationManager = getRaftInvocationManager(follower);
        ProtoOperation request = ProtoOperation.newBuilder().setSizeRequest(SizeRequest.getDefaultInstance()).build();
        CompletableFuture<Ordered<SizeResponse>> future = invocationManager.query(request, LEADER_LOCAL, 0);

        Ordered<SizeResponse> result = future.join();
        assertThat(result.getResult().getSize()).isEqualTo(1);
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testLeaderLocalQueryFromLeader() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        invokePutRequest(leader).join();

        RaftInvocationManager invocationManager = getRaftInvocationManager(leader);
        ProtoOperation request = ProtoOperation.newBuilder().setSizeRequest(SizeRequest.getDefaultInstance()).build();
        CompletableFuture<Ordered<SizeResponse>> future = invocationManager.query(request, LEADER_LOCAL, 0);

        Ordered<SizeResponse> result = future.join();
        assertThat(result.getResult().getSize()).isEqualTo(1);
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testAnyLocalQueryFromFollower() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        invokePutRequest(leader).join();

        AfloatDB follower = getAnyFollower(servers);

        RaftInvocationManager invocationManager = getRaftInvocationManager(follower);
        ProtoOperation request = ProtoOperation.newBuilder().setSizeRequest(SizeRequest.getDefaultInstance()).build();
        CompletableFuture<Ordered<SizeResponse>> future = invocationManager.query(request, ANY_LOCAL, 0);

        Ordered<SizeResponse> result = future.join();
        assertThat(result.getResult().getSize()).isLessThanOrEqualTo(1);
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    @Test
    public void testAnyLocalQueryFromLeader() {
        AfloatDB leader = waitUntilLeaderElected(servers);
        invokePutRequest(leader).join();

        RaftInvocationManager invocationManager = getRaftInvocationManager(leader);
        ProtoOperation request = ProtoOperation.newBuilder().setSizeRequest(SizeRequest.getDefaultInstance()).build();
        CompletableFuture<Ordered<SizeResponse>> future = invocationManager.query(request, ANY_LOCAL, 0);

        Ordered<SizeResponse> result = future.join();
        assertThat(result.getResult().getSize()).isEqualTo(1);
        assertThat(result.getCommitIndex()).isGreaterThan(0);
    }

    private CompletableFuture<Ordered<PutResponse>> invokePutRequest(AfloatDB server) {
        RaftInvocationManager invocationManager = getRaftInvocationManager(server);
        TypedValue val = TypedValue.newBuilder().setValue(ByteString.copyFromUtf8("val")).build();
        PutRequest request = PutRequest.newBuilder().setKey("key").setValue(val).build();
        ProtoOperation operation = ProtoOperation.newBuilder().setPutRequest(request).build();

        return invocationManager.invoke(operation);
    }

}
