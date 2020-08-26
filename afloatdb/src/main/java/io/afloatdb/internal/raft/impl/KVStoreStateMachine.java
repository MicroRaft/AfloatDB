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

package io.afloatdb.internal.raft.impl;

import io.afloatdb.kv.proto.ClearResponse;
import io.afloatdb.kv.proto.ContainsRequest;
import io.afloatdb.kv.proto.ContainsResponse;
import io.afloatdb.kv.proto.DeleteRequest;
import io.afloatdb.kv.proto.DeleteResponse;
import io.afloatdb.kv.proto.GetRequest;
import io.afloatdb.kv.proto.GetResponse;
import io.afloatdb.kv.proto.KVResponse;
import io.afloatdb.kv.proto.PutRequest;
import io.afloatdb.kv.proto.PutResponse;
import io.afloatdb.kv.proto.RemoveRequest;
import io.afloatdb.kv.proto.RemoveResponse;
import io.afloatdb.kv.proto.ReplaceRequest;
import io.afloatdb.kv.proto.ReplaceResponse;
import io.afloatdb.kv.proto.SetRequest;
import io.afloatdb.kv.proto.SetResponse;
import io.afloatdb.kv.proto.SizeResponse;
import io.afloatdb.kv.proto.TypedValue;
import io.afloatdb.raft.proto.KVEntry;
import io.afloatdb.raft.proto.KVSnapshotChunkData;
import io.afloatdb.raft.proto.Operation;
import io.afloatdb.raft.proto.StartNewTermOpProto;
import io.microraft.RaftEndpoint;
import io.microraft.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import static io.afloatdb.internal.di.AfloatDBModule.LOCAL_ENDPOINT_KEY;

@Singleton
public class KVStoreStateMachine
        implements StateMachine {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreStateMachine.class);

    // we need to keep the insertion order to create snapshot chunks
    // in a deterministic way on all servers.
    private final Map<String, TypedValue> map = new LinkedHashMap<>();

    private final RaftEndpoint localMember;

    @Inject
    public KVStoreStateMachine(@Named(LOCAL_ENDPOINT_KEY) RaftEndpoint localMember) {
        this.localMember = localMember;
    }

    @Override
    public Object runOperation(long commitIndex, @Nonnull Object operation) {
        if (!(operation instanceof Operation)) {
            throw new IllegalArgumentException("Invalid operation: " + operation + " at commit index: " + commitIndex);
        }

        Operation o = (Operation) operation;
        switch (o.getOperationCase()) {
            case STARTNEWTERMOP:
                return null;
            case PUTREQUEST:
                return put(commitIndex, o.getPutRequest());
            case SETREQUEST:
                return set(commitIndex, o.getSetRequest());
            case GETREQUEST:
                return get(commitIndex, o.getGetRequest());
            case CONTAINSREQUEST:
                return contains(commitIndex, o.getContainsRequest());
            case DELETEREQUEST:
                return delete(commitIndex, o.getDeleteRequest());
            case REMOVEREQUEST:
                return remove(commitIndex, o.getRemoveRequest());
            case REPLACEREQUEST:
                return replace(commitIndex, o.getReplaceRequest());
            case SIZEREQUEST:
                return size(commitIndex);
            case CLEARREQUEST:
                return clear(commitIndex);
            default:
                throw new IllegalArgumentException("Invalid operation: " + operation + " at commit index: " + commitIndex);
        }
    }

    private KVResponse put(long commitIndex, PutRequest request) {
        TypedValue prev;
        PutResponse.Builder builder = PutResponse.newBuilder();
        if (request.getAbsent()) {
            prev = map.putIfAbsent(request.getKey(), request.getValue());
        } else {
            prev = map.put(request.getKey(), request.getValue());
        }

        if (prev != null) {
            builder.setValue(prev);
        }

        return KVResponse.newBuilder().setCommitIndex(commitIndex).setPutResponse(builder.build()).build();
    }

    private KVResponse set(long commitIndex, SetRequest request) {
        map.put(request.getKey(), request.getValue());

        return KVResponse.newBuilder().setCommitIndex(commitIndex).setSetResponse(SetResponse.getDefaultInstance()).build();
    }

    private KVResponse get(long commitIndex, GetRequest request) {
        GetResponse.Builder builder = GetResponse.newBuilder();
        TypedValue val = map.get(request.getKey());
        if (val != null) {
            builder.setValue(val);
        }

        return KVResponse.newBuilder().setCommitIndex(commitIndex).setGetResponse(builder.build()).build();
    }

    private KVResponse contains(long commitIndex, ContainsRequest request) {
        boolean success;

        if (request.hasValue()) {
            success = request.getValue().equals(map.get(request.getKey()));
        } else {
            success = map.containsKey(request.getKey());
        }

        return KVResponse.newBuilder().setCommitIndex(commitIndex)
                         .setContainsResponse(ContainsResponse.newBuilder().setSuccess(success).build()).build();
    }

    private KVResponse delete(long commitIndex, DeleteRequest request) {
        boolean success = map.remove(request.getKey()) != null;

        return KVResponse.newBuilder().setCommitIndex(commitIndex)
                         .setDeleteResponse(DeleteResponse.newBuilder().setSuccess(success).build()).build();
    }

    private KVResponse remove(long commitIndex, RemoveRequest request) {
        RemoveResponse.Builder builder = RemoveResponse.newBuilder();
        boolean success;
        if (request.hasValue()) {
            success = map.remove(request.getKey(), request.getValue());
        } else {
            TypedValue val = map.remove(request.getKey());
            if (val != null) {
                builder.setValue(val);
            }

            success = val != null;
        }

        return KVResponse.newBuilder().setCommitIndex(commitIndex).setRemoveResponse(builder.setSuccess(success).build()).build();
    }

    private KVResponse replace(long commitIndex, ReplaceRequest request) {
        boolean success = map.replace(request.getKey(), request.getOldValue(), request.getNewValue());

        return KVResponse.newBuilder().setCommitIndex(commitIndex)
                         .setReplaceResponse(ReplaceResponse.newBuilder().setSuccess(success).build()).build();
    }

    private KVResponse size(long commitIndex) {
        int size = map.size();

        return KVResponse.newBuilder().setCommitIndex(commitIndex)
                         .setSizeResponse(SizeResponse.newBuilder().setSize(size).build()).build();
    }

    private KVResponse clear(long commitIndex) {
        int size = map.size();
        map.clear();

        return KVResponse.newBuilder().setCommitIndex(commitIndex)
                         .setClearResponse(ClearResponse.newBuilder().setSize(size).build()).build();
    }

    @Override
    public void takeSnapshot(long commitIndex, Consumer<Object> snapshotChunkConsumer) {
        KVSnapshotChunkData.Builder chunkBuilder = KVSnapshotChunkData.newBuilder();

        int chunkCount = 0, keyCount = 0;
        for (Entry<String, TypedValue> e : map.entrySet()) {
            keyCount++;
            KVEntry kvEntry = KVEntry.newBuilder().setKey(e.getKey()).setValue(e.getValue()).build();
            chunkBuilder.addEntry(kvEntry);
            if (chunkBuilder.getEntryCount() == 10000) {
                snapshotChunkConsumer.accept(chunkBuilder.build());
                chunkBuilder = KVSnapshotChunkData.newBuilder();
                chunkCount++;
            }
        }

        if (map.size() == 0 || chunkBuilder.getEntryCount() > 0) {
            snapshotChunkConsumer.accept(chunkBuilder.build());
            chunkCount++;
        }

        LOGGER.info("{} took snapshot with {} chunks and {} keys at log index: {}", localMember.getId(), chunkCount, keyCount,
                commitIndex);

        //        try {
        //            Output out = ByteString.newOutput();
        //            DataOutputStream os = new DataOutputStream(out);
        //            os.writeInt(map.size());
        //            for (Entry<ByteString, ByteString> entry : map.entrySet()) {
        //                ByteString key = entry.getKey();
        //                ByteString value = entry.getValue();
        //                os.writeInt(key.size());
        //                for (int i = 0; i < key.size(); i++) {
        //                    os.writeByte(key.byteAt(i));
        //                }
        //                os.writeInt(value.size());
        //                for (int i = 0; i < value.size(); i++) {
        //                    os.writeByte(value.byteAt(i));
        //                }
        //            }
        //            builder.setData(out.toByteString());
        //        } catch (IOException e) {
        //            throw new RuntimeException("Failure during take snapshot", e);
        //        }
    }

    @Override
    public void installSnapshot(long commitIndex, @Nonnull List<Object> snapshotChunks) {
        map.clear();

        for (Object chunk : snapshotChunks) {
            for (KVEntry entry : ((KVSnapshotChunkData) chunk).getEntryList()) {
                map.put(entry.getKey(), entry.getValue());
            }
        }

        //        try {
        //            ByteString bs = snapshot.getData();
        //            DataInputStream in = new DataInputStream(bs.newInput());
        //            int entryCount = in.readInt();
        //            for (int i = 0; i < entryCount; i++) {
        //                int keySize = in.readInt();
        //                ByteString key = readBytes(in, keySize);
        //                int valueSize = in.readInt();
        //                ByteString value = readBytes(in, valueSize);
        //                map.put(key, value);
        //            }
        //        } catch (IOException e) {
        //            throw new RuntimeException("Failure during snapshot restore", e);
        //        }

        LOGGER.info("{} restored snapshot with {} keys at commit index: {}", localMember.getId(), map.size(), commitIndex);
    }

    //    private ByteString readBytes(DataInputStream in, int count) throws IOException {
    //        Output out = ByteString.newOutput(count);
    //        for (int j = 0; j < count; j++) {
    //            out.write(in.readByte());
    //        }
    //
    //        return out.toByteString();
    //    }

    @Nonnull
    @Override
    public Object getNewTermOperation() {
        return Operation.newBuilder().setStartNewTermOp(StartNewTermOpProto.getDefaultInstance()).build();
    }

}
