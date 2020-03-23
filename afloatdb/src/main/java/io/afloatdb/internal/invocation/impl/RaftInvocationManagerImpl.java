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

package io.afloatdb.internal.invocation.impl;

import io.afloatdb.internal.invocation.RaftInvocationManager;
import io.afloatdb.internal.raft.RaftNodeReportObserver;
import io.afloatdb.internal.rpc.RaftRpcStub;
import io.afloatdb.internal.rpc.RaftRpcStubManager;
import io.afloatdb.internal.utils.Exceptions;
import io.afloatdb.raft.proto.ProtoOperation;
import io.afloatdb.raft.proto.ProtoOperationResponse;
import io.afloatdb.raft.proto.ProtoQueryRequest;
import io.afloatdb.raft.proto.ProtoReplicateRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.impl.util.OrderedFuture;
import io.microraft.report.RaftNodeReport;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static io.afloatdb.internal.di.AfloatDBModule.RAFT_NODE_SUPPLIER_KEY;
import static io.afloatdb.internal.utils.Serialization.toProto;
import static io.afloatdb.internal.utils.Serialization.unwrapResponse;
import static io.grpc.Status.FAILED_PRECONDITION;
import static io.grpc.Status.RESOURCE_EXHAUSTED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Singleton
public class RaftInvocationManagerImpl
        implements RaftInvocationManager {

    // TODO [basri] make it configurable
    private static final int RETRY_LIMIT = 10;

    private final RaftNode raftNode;
    private final Supplier<RaftNodeReport> raftNodeReportSupplier;
    private final RaftRpcStubManager raftRpcStubManager;
    private final ScheduledExecutorService executor;

    @Inject
    public RaftInvocationManagerImpl(@Named(RAFT_NODE_SUPPLIER_KEY) Supplier<RaftNode> raftNodeSupplier,
                                     RaftNodeReportObserver raftNodeReportObserver, RaftRpcStubManager raftRpcStubManager) {
        this.raftNode = raftNodeSupplier.get();
        this.raftNodeReportSupplier = raftNodeReportObserver;
        this.raftRpcStubManager = raftRpcStubManager;
        this.executor = newSingleThreadScheduledExecutor();
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }

    @Override
    public <T> CompletableFuture<Ordered<T>> invoke(@Nonnull ProtoOperation operation) {
        requireNonNull(operation);

        ProtoReplicateRequest request = ProtoReplicateRequest.newBuilder().setOperation(operation).build();
        Invocation<ProtoReplicateRequest, T> invocation = new ReplicateInvocation<>(request);
        invocation.invoke();

        return invocation;
    }

    @Override
    public <T> CompletableFuture<Ordered<T>> query(@Nonnull ProtoOperation operation, @Nonnull QueryPolicy queryPolicy,
                                                   long minCommitRequest) {
        requireNonNull(operation);
        requireNonNull(queryPolicy);

        ProtoQueryRequest request = ProtoQueryRequest.newBuilder().setOperation(operation).setQueryPolicy(toProto(queryPolicy))
                                                     .setMinCommitIndex(minCommitRequest).build();

        QueryInvocation<T> invocation = new QueryInvocation<>(request, queryPolicy);
        invocation.invoke();

        return invocation;
    }

    private <T> void failFutureIfNotRetryable(OrderedFuture<T> future, Throwable t) {
        if (!t.getMessage().startsWith("RAFT") || !(t instanceof StatusRuntimeException)) {
            future.fail(t);
            return;
        }

        StatusRuntimeException ex = (StatusRuntimeException) t;
        if (!(ex.getStatus() == FAILED_PRECONDITION || ex.getStatus() == RESOURCE_EXHAUSTED)) {
            future.fail(t);
        }
    }

    abstract class Invocation<Req, Resp>
            extends OrderedFuture<Resp>
            implements StreamObserver<ProtoOperationResponse>, BiConsumer<Ordered<Resp>, Throwable> {

        final Req request;
        volatile int tryCount;

        Invocation(Req request) {
            this.request = request;
        }

        final void retry() {
            ++tryCount;
            if (tryCount >= RETRY_LIMIT) {
                fail(new StatusRuntimeException(Status.UNAVAILABLE));
            } else if (tryCount > 5) {
                executor.schedule(this::invoke, 500, MILLISECONDS);
            } else if (tryCount > 3) {
                executor.schedule(this::invoke, 100, MILLISECONDS);
            } else {
                executor.schedule(this::invoke, 1, MILLISECONDS);
            }
        }

        @Override
        public final void onNext(ProtoOperationResponse response) {
            complete(response.getCommitIndex(), (Resp) unwrapResponse(response));
        }

        @Override
        public final void onError(Throwable t) {
            failFutureIfNotRetryable(this, Exceptions.wrap(t));
        }

        @Override
        public final void onCompleted() {
            if (!isDone()) {
                retry();
            }
        }

        @Override
        public final void accept(Ordered<Resp> ordered, Throwable throwable) {
            if (throwable == null) {
                complete(ordered.getCommitIndex(), ordered.getResult());
            } else {
                onError(throwable);
                onCompleted();
            }
        }

        final void invoke() {
            if (!invokeLocally()) {
                invokeRemotely();
            }
        }

        final void invokeRemotely() {
            RaftNodeReport report = raftNodeReportSupplier.get();
            if (report != null) {
                RaftEndpoint leader = report.getTerm().getLeaderEndpoint();
                if (leader != null) {
                    RaftRpcStub stub = raftRpcStubManager.getRpcStub(leader);
                    if (stub != null) {
                        doInvokeRemotely(stub);
                        return;
                    }
                }
            }

            retry();
        }

        abstract boolean invokeLocally();

        abstract void doInvokeRemotely(RaftRpcStub stub);

    }

    class ReplicateInvocation<Resp>
            extends Invocation<ProtoReplicateRequest, Resp> {
        ReplicateInvocation(ProtoReplicateRequest request) {
            super(request);
        }

        @Override
        boolean invokeLocally() {
            if (raftNode.getLocalEndpoint().equals(raftNode.getTerm().getLeaderEndpoint())) {
                raftNode.<Resp>replicate(request.getOperation()).whenComplete(this);
                return true;
            }

            return false;
        }

        @Override
        protected void doInvokeRemotely(RaftRpcStub stub) {
            // TODO [basri] offload to IO thread...
            stub.replicate(request, this);
        }
    }

    class QueryInvocation<Resp>
            extends Invocation<ProtoQueryRequest, Resp> {
        final QueryPolicy queryPolicy;

        QueryInvocation(ProtoQueryRequest request, QueryPolicy queryPolicy) {
            super(request);
            this.queryPolicy = queryPolicy;
        }

        @Override
        boolean invokeLocally() {
            if (queryPolicy == QueryPolicy.ANY_LOCAL || raftNode.getLocalEndpoint()
                                                                .equals(raftNode.getTerm().getLeaderEndpoint())) {
                raftNode.<Resp>query(request.getOperation(), queryPolicy, request.getMinCommitIndex()).whenComplete(this);
                return true;
            }

            return false;
        }

        @Override
        void doInvokeRemotely(RaftRpcStub stub) {
            // TODO [basri] offload to IO thread...
            stub.query(request, this);
        }
    }

}
