package io.afloatdb.internal.invocation.impl;

import io.afloatdb.kv.proto.KVResponse;
import io.afloatdb.raft.proto.QueryRequest;
import io.afloatdb.raft.proto.RaftInvocationHandlerGrpc.RaftInvocationHandlerImplBase;
import io.afloatdb.raft.proto.ReplicateRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.microraft.QueryPolicy;
import io.microraft.RaftNode;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.function.Supplier;

import static io.afloatdb.internal.di.AfloatDBModule.RAFT_NODE_SUPPLIER_KEY;
import static io.afloatdb.internal.utils.Exceptions.wrap;
import static io.afloatdb.internal.utils.Serialization.fromProto;

@Singleton
public class RaftInvocationHandler extends RaftInvocationHandlerImplBase {

    private final RaftNode raftNode;

    @Inject
    public RaftInvocationHandler(@Named(RAFT_NODE_SUPPLIER_KEY) Supplier<RaftNode> raftNodeSupplier) {
        this.raftNode = raftNodeSupplier.get();
    }

    @Override
    public void replicate(ReplicateRequest request, StreamObserver<KVResponse> responseObserver) {
        // TODO [basri] offload to io thread pool
        raftNode.<KVResponse> replicate(request.getOperation()).whenComplete((response, throwable) -> {
            if (throwable == null) {
                responseObserver.onNext(response.getResult());
            } else {
                responseObserver.onError(wrap(throwable));
            }
            responseObserver.onCompleted();
        });
    }

    @Override
    public void query(QueryRequest request, StreamObserver<KVResponse> responseObserver) {
        QueryPolicy queryPolicy = fromProto(request.getQueryPolicy());
        if (queryPolicy == null) {
            responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
            responseObserver.onCompleted();
            return;
        }

        // TODO [basri] offload to io thread pool
        raftNode.<KVResponse> query(request.getOperation(), queryPolicy, request.getMinCommitIndex())
                .whenComplete((response, throwable) -> {
                    if (throwable == null) {
                        responseObserver.onNext(response.getResult());
                    } else {
                        responseObserver.onError(wrap(throwable));
                    }
                    responseObserver.onCompleted();
                });
    }

}
