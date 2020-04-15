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

package io.afloatdb.client.internal.kvstore.impl;

import io.afloatdb.client.kvstore.KV;
import io.afloatdb.internal.serialization.Serialization;
import io.afloatdb.kv.proto.ClearRequest;
import io.afloatdb.kv.proto.ClearResponse;
import io.afloatdb.kv.proto.ContainsRequest;
import io.afloatdb.kv.proto.ContainsResponse;
import io.afloatdb.kv.proto.DeleteRequest;
import io.afloatdb.kv.proto.DeleteResponse;
import io.afloatdb.kv.proto.GetRequest;
import io.afloatdb.kv.proto.GetResponse;
import io.afloatdb.kv.proto.KVServiceGrpc.KVServiceBlockingStub;
import io.afloatdb.kv.proto.PutRequest;
import io.afloatdb.kv.proto.PutResponse;
import io.afloatdb.kv.proto.RemoveRequest;
import io.afloatdb.kv.proto.RemoveResponse;
import io.afloatdb.kv.proto.ReplaceRequest;
import io.afloatdb.kv.proto.ReplaceResponse;
import io.afloatdb.kv.proto.SetRequest;
import io.afloatdb.kv.proto.SizeRequest;
import io.afloatdb.kv.proto.SizeResponse;
import io.afloatdb.kv.proto.TypedValue;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

import static io.afloatdb.internal.serialization.Serialization.BYTE_ARRAY_TYPE;
import static io.afloatdb.internal.serialization.Serialization.INT_TYPE;
import static io.afloatdb.internal.serialization.Serialization.LONG_TYPE;
import static io.afloatdb.internal.serialization.Serialization.STRING_TYPE;
import static io.afloatdb.internal.serialization.Serialization.serializeBytes;
import static io.afloatdb.internal.serialization.Serialization.serializeInt;
import static io.afloatdb.internal.serialization.Serialization.serializeLong;
import static io.afloatdb.internal.serialization.Serialization.serializeString;
import static java.util.Objects.requireNonNull;

// TODO [basri] wrap grpc exceptions
public class KVProxy
        implements KV {

    private final Supplier<KVServiceBlockingStub> kvStubSupplier;

    public KVProxy(Supplier<KVServiceBlockingStub> kvStubSupplier) {
        this.kvStubSupplier = kvStubSupplier;
    }

    @Override
    public <T> T put(@Nonnull String key, @Nonnull byte[] value) {
        return put(requireNonNull(key), getTypedValue(requireNonNull(value)), false);
    }

    @Override
    public <T> T put(@Nonnull String key, int value) {
        return put(requireNonNull(key), getTypedValue(value), false);
    }

    @Override
    public <T> T put(@Nonnull String key, long value) {
        return put(requireNonNull(key), getTypedValue(value), false);
    }

    @Override
    public <T> T put(@Nonnull String key, @Nonnull String value) {
        return put(requireNonNull(key), getTypedValue(requireNonNull(value)), false);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, @Nonnull byte[] value) {
        return put(requireNonNull(key), getTypedValue(requireNonNull(value)), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, int value) {
        return put(requireNonNull(key), getTypedValue(value), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, long value) {
        return put(requireNonNull(key), getTypedValue(value), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, @Nonnull String value) {
        return put(requireNonNull(key), getTypedValue(requireNonNull(value)), true);
    }

    private TypedValue getTypedValue(@Nonnull String value) {
        return TypedValue.newBuilder().setType(STRING_TYPE).setValue(serializeString(value)).build();
    }

    private TypedValue getTypedValue(long value) {
        return TypedValue.newBuilder().setType(LONG_TYPE).setValue(serializeLong(value)).build();
    }

    private TypedValue getTypedValue(int value) {
        return TypedValue.newBuilder().setType(INT_TYPE).setValue(serializeInt(value)).build();
    }

    private TypedValue getTypedValue(byte[] value) {
        return TypedValue.newBuilder().setType(BYTE_ARRAY_TYPE).setValue(serializeBytes(value)).build();
    }

    private <T> T put(String key, TypedValue value, boolean absent) {
        PutRequest request = PutRequest.newBuilder().setKey(key).setValue(value).setAbsent(absent).build();
        PutResponse response = kvStubSupplier.get().put(request);
        return response.hasValue() ? (T) Serialization.deserialize(response.getValue()) : null;
    }

    @Override
    public void set(String key, @Nonnull byte[] value) {
        set(requireNonNull(key), getTypedValue(requireNonNull(value)));
    }

    @Override
    public void set(String key, int value) {
        set(requireNonNull(key), getTypedValue(value));
    }

    @Override
    public void set(String key, long value) {
        set(requireNonNull(key), getTypedValue(value));
    }

    @Override
    public void set(String key, @Nonnull String value) {
        set(requireNonNull(key), getTypedValue(requireNonNull(value)));
    }

    private void set(String key, TypedValue value) {
        SetRequest request = SetRequest.newBuilder().setKey(key).setValue(value).build();
        kvStubSupplier.get().set(request);
    }

    @Override
    public <T> T get(String key) {
        requireNonNull(key);

        GetRequest request = GetRequest.newBuilder().setKey(key).build();
        GetResponse response = kvStubSupplier.get().get(request);
        return response.hasValue() ? (T) Serialization.deserialize(response.getValue()) : null;
    }

    @Override
    public boolean contains(@Nonnull String key) {
        requireNonNull(key);

        ContainsRequest request = ContainsRequest.newBuilder().setKey(key).build();
        ContainsResponse response = kvStubSupplier.get().contains(request);
        return response.getSuccess();
    }

    @Override
    public boolean contains(@Nonnull String key, @Nonnull byte[] value) {
        return contains(requireNonNull(key), getTypedValue(requireNonNull(value)));
    }

    @Override
    public boolean contains(@Nonnull String key, int value) {
        return contains(requireNonNull(key), getTypedValue(value));
    }

    @Override
    public boolean contains(@Nonnull String key, long value) {
        return contains(requireNonNull(key), getTypedValue(value));
    }

    @Override
    public boolean contains(@Nonnull String key, @Nonnull String value) {
        return contains(requireNonNull(key), getTypedValue(value));
    }

    private boolean contains(String key, TypedValue value) {
        ContainsRequest request = ContainsRequest.newBuilder().setKey(key).setValue(value).build();
        return kvStubSupplier.get().contains(request).getSuccess();
    }

    @Override
    public boolean delete(@Nonnull String key) {
        requireNonNull(key);

        DeleteRequest request = DeleteRequest.newBuilder().setKey(key).build();
        DeleteResponse response = kvStubSupplier.get().delete(request);
        return response.getSuccess();
    }

    @Override
    public <T> T remove(@Nonnull String key) {
        requireNonNull(key);

        RemoveRequest request = RemoveRequest.newBuilder().setKey(key).build();
        RemoveResponse response = kvStubSupplier.get().remove(request);
        return response.hasValue() ? (T) Serialization.deserialize(response.getValue()) : null;
    }

    @Override
    public boolean remove(@Nonnull String key, byte[] value) {
        return remove(requireNonNull(key), getTypedValue(requireNonNull(value)));
    }

    @Override
    public boolean remove(@Nonnull String key, int value) {
        return remove(requireNonNull(key), getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, long value) {
        return remove(requireNonNull(key), getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, @Nonnull String value) {
        return remove(requireNonNull(key), getTypedValue(value));
    }

    private boolean remove(String key, TypedValue value) {
        RemoveRequest request = RemoveRequest.newBuilder().setKey(key).setValue(value).build();
        return kvStubSupplier.get().remove(request).getSuccess();
    }

    @Override
    public boolean replace(@Nonnull String key, @Nonnull Object oldValue, @Nonnull Object newValue) {
        requireNonNull(key);
        requireNonNull(oldValue);
        requireNonNull(newValue);

        ReplaceRequest request = ReplaceRequest.newBuilder().setKey(key).setOldValue(getTypedValue(oldValue))
                                               .setNewValue(getTypedValue(newValue)).build();
        ReplaceResponse response = kvStubSupplier.get().replace(request);
        return response.getSuccess();
    }

    private TypedValue getTypedValue(Object object) {
        if (object instanceof byte[]) {
            return getTypedValue((byte[]) object);
        } else if (object instanceof Integer) {
            return getTypedValue((int) object);
        } else if (object instanceof Long) {
            return getTypedValue((long) object);
        } else if (object instanceof String) {
            return getTypedValue((String) object);
        }

        throw new IllegalArgumentException(object + " has invalid type!");
    }

    @Override
    public int size() {
        SizeResponse response = kvStubSupplier.get().size(SizeRequest.getDefaultInstance());
        return response.getSize();
    }

    @Override
    public int clear() {
        ClearResponse response = kvStubSupplier.get().clear(ClearRequest.getDefaultInstance());
        return response.getSize();
    }

}
