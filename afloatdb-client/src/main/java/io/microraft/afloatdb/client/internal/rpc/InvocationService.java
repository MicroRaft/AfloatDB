package io.microraft.afloatdb.client.internal.rpc;

import com.google.common.util.concurrent.ListenableFuture;
import io.microraft.afloatdb.kv.proto.KVResponse;
import io.microraft.afloatdb.kv.proto.KVServiceGrpc.KVServiceFutureStub;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface InvocationService {
    CompletableFuture<KVResponse> invoke(Function<KVServiceFutureStub, ListenableFuture<KVResponse>> func);
}
