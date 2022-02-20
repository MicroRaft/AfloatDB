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

import com.google.common.util.concurrent.ListenableFuture;
import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.internal.invocation.InvocationService;
import io.afloatdb.internal.raft.RaftNodeReportSupplier;
import io.afloatdb.internal.rpc.RaftRpc;
import io.afloatdb.internal.rpc.RaftRpcService;
import io.afloatdb.internal.utils.Exceptions;
import io.afloatdb.kv.proto.KVResponse;
import io.afloatdb.raft.proto.Operation;
import io.afloatdb.raft.proto.QueryRequest;
import io.afloatdb.raft.proto.ReplicateRequest;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.impl.util.OrderedFuture;
import io.microraft.report.RaftNodeReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static io.afloatdb.internal.di.AfloatDBModule.CONFIG_KEY;
import static io.afloatdb.internal.di.AfloatDBModule.RAFT_NODE_SUPPLIER_KEY;
import static io.afloatdb.internal.utils.Exceptions.isRaftException;
import static io.afloatdb.internal.utils.Serialization.toProto;
import static io.grpc.Status.RESOURCE_EXHAUSTED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Singleton
public class InvocationServiceImpl implements InvocationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(InvocationService.class);

    private final RaftNode raftNode;
    private final Supplier<RaftNodeReport> raftNodeReportSupplier;
    private final RaftRpcService raftRpcService;
    private final ScheduledExecutorService executor;
    private final long retryLimit;

    @Inject
    public InvocationServiceImpl(@Named(CONFIG_KEY) AfloatDBConfig config,
            @Named(RAFT_NODE_SUPPLIER_KEY) Supplier<RaftNode> raftNodeSupplier,
            RaftNodeReportSupplier raftNodeReportSupplier, RaftRpcService raftRpcService) {
        this.raftNode = raftNodeSupplier.get();
        this.raftNodeReportSupplier = raftNodeReportSupplier;
        this.raftRpcService = raftRpcService;
        this.executor = newSingleThreadScheduledExecutor();
        this.retryLimit = config.getRpcConfig().getRetryLimit();
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }

    @Override
    public CompletableFuture<Ordered<KVResponse>> invoke(@Nonnull Operation operation) {
        return new ReplicateInvocation(operation).invoke();
    }

    @Override
    public CompletableFuture<Ordered<KVResponse>> query(@Nonnull Operation operation, @Nonnull QueryPolicy queryPolicy,
            long minCommitIndex) {
        return new QueryInvocation(operation, queryPolicy, minCommitIndex).invoke();
    }

    private abstract class Invocation extends OrderedFuture<KVResponse>
            implements BiConsumer<Ordered<KVResponse>, Throwable> {

        final Operation operation;
        volatile int retryCount;

        Invocation(Operation operation) {
            this.operation = requireNonNull(operation);
        }

        final void retry() {
            // TODO [basri] poor man's retry
            ++retryCount;
            if (retryCount > retryLimit) {
                fail(RESOURCE_EXHAUSTED.withDescription("Retry limit exceeded.").asRuntimeException());
            } else if (retryCount > 10) {
                executor.schedule(this::invoke, 250, MILLISECONDS);
            } else if (retryCount > 5) {
                executor.schedule(this::invoke, 50, MILLISECONDS);
            } else if (retryCount > 3) {
                executor.schedule(this::invoke, 10, MILLISECONDS);
            } else {
                executor.submit(this::invoke);
            }
        }

        @Override
        public final void accept(Ordered<KVResponse> ordered, Throwable t) {
            if (t == null) {
                complete(ordered.getCommitIndex(), ordered.getResult());
            } else {
                failOrRetry(this, Exceptions.wrap(t));
            }
        }

        final CompletableFuture<Ordered<KVResponse>> invoke() {
            CompletableFuture<Ordered<KVResponse>> f = tryInvokeLocally();
            if (f != null) {
                f.whenComplete(this);
            } else {
                invokeRemotely();
            }

            return this;
        }

        final void invokeRemotely() {
            RaftNodeReport report = raftNodeReportSupplier.get();
            if (report != null) {
                RaftEndpoint leader = report.getTerm().getLeaderEndpoint();
                if (leader != null) {
                    RaftRpc stub = raftRpcService.getRpcStub(leader);
                    if (stub != null) {
                        handleResult(doInvokeRemotely(stub));
                        return;
                    }
                }
            }

            retry();
        }

        private void handleResult(ListenableFuture<KVResponse> future) {
            future.addListener(() -> {
                try {
                    KVResponse response = future.get();
                    complete(response.getCommitIndex(), response);
                } catch (Throwable t) {
                    if (t instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }

                    if (t instanceof ExecutionException) {
                        t = t.getCause();
                    }

                    failOrRetry(this, Exceptions.wrap(t));
                }

            }, executor);
        }

        private <T> void failOrRetry(OrderedFuture<T> future, Throwable t) {
            if (t instanceof StatusRuntimeException && isRaftException(t.getMessage())) {
                StatusRuntimeException ex = (StatusRuntimeException) t;
                if (ex.getStatus().getCode() == Code.FAILED_PRECONDITION
                        || ex.getStatus().getCode() == Code.RESOURCE_EXHAUSTED) {
                    retry();
                    return;
                }
            }

            future.fail(t);
        }

        abstract CompletableFuture<Ordered<KVResponse>> tryInvokeLocally();

        abstract ListenableFuture<KVResponse> doInvokeRemotely(RaftRpc stub);

    }

    private class ReplicateInvocation extends Invocation {

        // no need to make it volatile.
        // it is ok for multiple threads to create it redundantly
        ReplicateRequest request;

        ReplicateInvocation(Operation operation) {
            super(operation);
        }

        @Override
        CompletableFuture<Ordered<KVResponse>> tryInvokeLocally() {
            if (raftNode.getLocalEndpoint().equals(raftNode.getTerm().getLeaderEndpoint())) {
                return raftNode.replicate(operation);
            }

            return null;
        }

        @Override
        protected ListenableFuture<KVResponse> doInvokeRemotely(RaftRpc stub) {
            if (request == null) {
                request = ReplicateRequest.newBuilder().setOperation(operation).build();
            }

            // TODO [basri] offload to IO thread...
            return stub.replicate(request);
        }
    }

    private class QueryInvocation extends Invocation {

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
        CompletableFuture<Ordered<KVResponse>> tryInvokeLocally() {
            if (queryPolicy == QueryPolicy.EVENTUAL_CONSISTENCY || queryPolicy == QueryPolicy.BOUNDED_STALENESS
                    || raftNode.getLocalEndpoint().equals(raftNode.getTerm().getLeaderEndpoint())) {
                return raftNode.query(operation, queryPolicy, minCommitIndex);
            }

            return null;
        }

        @Override
        ListenableFuture<KVResponse> doInvokeRemotely(RaftRpc stub) {
            if (request == null) {
                request = QueryRequest.newBuilder().setOperation(operation).setQueryPolicy(toProto(queryPolicy))
                        .setMinCommitIndex(minCommitIndex).build();
            }
            // TODO [basri] offload to IO thread...
            return stub.query(request);
        }
    }

}
