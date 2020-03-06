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

package io.afloatdb.internal.raft.impl;

import io.afloatdb.kv.proto.ClearResponse;
import io.afloatdb.kv.proto.ContainsRequest;
import io.afloatdb.kv.proto.ContainsResponse;
import io.afloatdb.kv.proto.DeleteRequest;
import io.afloatdb.kv.proto.DeleteResponse;
import io.afloatdb.kv.proto.GetRequest;
import io.afloatdb.kv.proto.GetResponse;
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
import io.afloatdb.raft.proto.ProtoKVEntry;
import io.afloatdb.raft.proto.ProtoKVSnapshotChunkObject;
import io.afloatdb.raft.proto.ProtoOperation;
import io.afloatdb.raft.proto.ProtoStartNewTermOp;
import io.microraft.RaftEndpoint;
import io.microraft.integration.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import static io.afloatdb.internal.di.AfloatDBModule.LOCAL_ENDPOINT_KEY;

@Singleton
public class KVStoreStateMachine
        implements StateMachine {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreStateMachine.class);

    private final Map<String, TypedValue> map = new HashMap<>();

    private final RaftEndpoint localMember;

    @Inject
    public KVStoreStateMachine(@Named(LOCAL_ENDPOINT_KEY) RaftEndpoint localMember) {
        this.localMember = localMember;
    }

    @Override
    public Object runOperation(long commitIndex, @Nullable Object operation) {
        if (!(operation instanceof ProtoOperation)) {
            throw new IllegalArgumentException("Invalid operation: " + operation + " at commit index: " + commitIndex);
        }

        ProtoOperation proto = (ProtoOperation) operation;
        switch (proto.getOperationCase()) {
            case STARTNEWTERMOP:
                return null;
            case PUTREQUEST:
                return put(proto.getPutRequest());
            case SETREQUEST:
                return set(proto.getSetRequest());
            case GETREQUEST:
                return get(proto.getGetRequest());
            case CONTAINSREQUEST:
                return contains(proto.getContainsRequest());
            case DELETEREQUEST:
                return delete(proto.getDeleteRequest());
            case REMOVEREQUEST:
                return remove(proto.getRemoveRequest());
            case REPLACEREQUEST:
                return replace(proto.getReplaceRequest());
            case SIZEREQUEST:
                return size();
            case CLEARREQUEST:
                return clear();
            default:
                return operation;
        }
    }

    private Object put(PutRequest request) {
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

        return builder.build();
    }

    private Object set(SetRequest request) {
        map.put(request.getKey(), request.getValue());
        return SetResponse.getDefaultInstance();
    }

    private Object get(GetRequest request) {
        GetResponse.Builder builder = GetResponse.newBuilder();
        TypedValue val = map.get(request.getKey());
        if (val != null) {
            builder.setValue(val);
        }

        return builder.build();
    }

    private Object contains(ContainsRequest request) {
        boolean success;

        if (request.hasValue()) {
            success = request.getValue().equals(map.get(request.getKey()));
        } else {
            success = map.containsKey(request.getKey());
        }

        return ContainsResponse.newBuilder().setSuccess(success).build();
    }

    private Object delete(DeleteRequest request) {
        boolean success = map.remove(request.getKey()) != null;
        return DeleteResponse.newBuilder().setSuccess(success).build();
    }

    private Object remove(RemoveRequest request) {
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

        return builder.setSuccess(success).build();
    }

    private Object replace(ReplaceRequest request) {
        boolean success = map.replace(request.getKey(), request.getOldValue(), request.getNewValue());
        return ReplaceResponse.newBuilder().setSuccess(success).build();
    }

    private Object size() {
        int size = map.size();
        return SizeResponse.newBuilder().setSize(size).build();
    }

    private Object clear() {
        int size = map.size();
        map.clear();
        return ClearResponse.newBuilder().setSize(size).build();
    }

    @Override
    public void takeSnapshot(long commitIndex, Consumer<Object> snapshotChunkConsumer) {
        ProtoKVSnapshotChunkObject.Builder builder = ProtoKVSnapshotChunkObject.newBuilder();

        for (Entry<String, TypedValue> entry : map.entrySet()) {
            ProtoKVEntry protoEntry = ProtoKVEntry.newBuilder().setKey(entry.getKey()).setValue(entry.getValue()).build();
            builder.addEntry(protoEntry);
            if (builder.getEntryCount() == 5000) {
                snapshotChunkConsumer.accept(builder.build());
                builder = ProtoKVSnapshotChunkObject.newBuilder();
            }
        }

        if (map.size() == 0 || builder.getEntryCount() > 0) {
            snapshotChunkConsumer.accept(builder.build());
        }

        LOGGER.info("{} took snapshot at commit index: {}", localMember.getId(), commitIndex);

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
            for (ProtoKVEntry entry : ((ProtoKVSnapshotChunkObject) chunk).getEntryList()) {
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

        LOGGER.info("{} restored snapshot at commit index: {}", localMember.getId(), commitIndex);
    }

    //    private ByteString readBytes(DataInputStream in, int count) throws IOException {
    //        Output out = ByteString.newOutput(count);
    //        for (int j = 0; j < count; j++) {
    //            out.write(in.readByte());
    //        }
    //
    //        return out.toByteString();
    //    }

    @Nullable
    @Override
    public Object getNewTermOperation() {
        return ProtoOperation.newBuilder().setStartNewTermOp(ProtoStartNewTermOp.getDefaultInstance()).build();
    }

}
