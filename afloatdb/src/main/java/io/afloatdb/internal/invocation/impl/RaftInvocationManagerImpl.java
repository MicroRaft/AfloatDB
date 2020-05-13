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
import io.afloatdb.raft.proto.Operation;
import io.afloatdb.raft.proto.OperationResponse;
import io.afloatdb.raft.proto.QueryRequest;
import io.afloatdb.raft.proto.ReplicateRequest;
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
    public <T> CompletableFuture<Ordered<T>> invoke(@Nonnull Operation operation) {
        Invocation<T> invocation = new ReplicateInvocation<T>(operation);
        invocation.invoke();

        return invocation;
    }

    @Override
    public <T> CompletableFuture<Ordered<T>> query(@Nonnull Operation operation, @Nonnull QueryPolicy queryPolicy,
                                                   long minCommitIndex) {
        QueryInvocation<T> invocation = new QueryInvocation<>(operation, queryPolicy, minCommitIndex);
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

    abstract class Invocation<Resp>
            extends OrderedFuture<Resp>
            implements StreamObserver<OperationResponse>, BiConsumer<Ordered<Resp>, Throwable> {

        final Operation operation;
        volatile int tryCount;

        Invocation(Operation operation) {
            this.operation = requireNonNull(operation);
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
        public final void onNext(OperationResponse response) {
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
            extends Invocation<Resp> {

        // no need to make it volatile.
        // it is ok for multiple threads to create it redundantly
        ReplicateRequest request;

        ReplicateInvocation(Operation operation) {
            super(operation);
        }

        @Override
        boolean invokeLocally() {
            if (raftNode.getLocalEndpoint().equals(raftNode.getTerm().getLeaderEndpoint())) {
                raftNode.<Resp>replicate(operation).whenComplete(this);
                return true;
            }

            return false;
        }

        @Override
        protected void doInvokeRemotely(RaftRpcStub stub) {
            if (request == null) {
                request = ReplicateRequest.newBuilder().setOperation(operation).build();
            }
            // TODO [basri] offload to IO thread...
            stub.replicate(request, this);
        }
    }

    class QueryInvocation<Resp>
            extends Invocation<Resp> {

        final QueryPolicy queryPolicy;
        final long minCommitIndex;

        // no need to make it volatile.
        // it is ok for multiple threads to create it redundantly
        QueryRequest request;

        QueryInvocation(Operation operation, QueryPolicy queryPolicy, long minCommitIndex) {
            super(operation);
            this.queryPolicy = requireNonNull(queryPolicy);
            this.minCommitIndex = minCommitIndex;
        }

        @Override
        boolean invokeLocally() {
            if (queryPolicy == QueryPolicy.ANY_LOCAL || raftNode.getLocalEndpoint()
                                                                .equals(raftNode.getTerm().getLeaderEndpoint())) {
                raftNode.<Resp>query(operation, queryPolicy, minCommitIndex).whenComplete(this);
                return true;
            }

            return false;
        }

        @Override
        void doInvokeRemotely(RaftRpcStub stub) {
            if (request == null) {
                request = QueryRequest.newBuilder().setOperation(operation).setQueryPolicy(toProto(queryPolicy))
                                      .setMinCommitIndex(minCommitIndex).build();
            }
            // TODO [basri] offload to IO thread...
            stub.query(request, this);
        }
    }

}
