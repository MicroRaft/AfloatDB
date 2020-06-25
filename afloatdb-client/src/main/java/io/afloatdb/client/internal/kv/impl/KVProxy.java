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

package io.afloatdb.client.internal.kv.impl;

import io.afloatdb.client.kv.KV;
import io.afloatdb.client.kv.Ordered;
import io.afloatdb.internal.serialization.Serialization;
import io.afloatdb.kv.proto.ClearRequest;
import io.afloatdb.kv.proto.ClearResponse;
import io.afloatdb.kv.proto.ContainsRequest;
import io.afloatdb.kv.proto.ContainsResponse;
import io.afloatdb.kv.proto.DeleteRequest;
import io.afloatdb.kv.proto.DeleteResponse;
import io.afloatdb.kv.proto.GetRequest;
import io.afloatdb.kv.proto.GetResponse;
import io.afloatdb.kv.proto.KVRequestHandlerGrpc.KVRequestHandlerBlockingStub;
import io.afloatdb.kv.proto.KVResponse;
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

public class KVProxy
        implements KV {

    private final Supplier<KVRequestHandlerBlockingStub> kvStubSupplier;

    public KVProxy(Supplier<KVRequestHandlerBlockingStub> kvStubSupplier) {
        this.kvStubSupplier = kvStubSupplier;
    }

    @Override
    public <T> T put(@Nonnull String key, @Nonnull byte[] value) {
        return put(key, getTypedValue(requireNonNull(value)), false);
    }

    @Nonnull
    @Override
    public <T> Ordered<T> putOrdered(@Nonnull String key, @Nonnull byte[] value) {
        return putOrdered(key, getTypedValue(requireNonNull(value)), false);
    }

    @Override
    public <T> T put(@Nonnull String key, int value) {
        return put(key, getTypedValue(value), false);
    }

    @Nonnull
    @Override
    public <T> Ordered<T> putOrdered(@Nonnull String key, int value) {
        return putOrdered(key, getTypedValue(value), false);
    }

    @Override
    public <T> T put(@Nonnull String key, long value) {
        return put(key, getTypedValue(value), false);
    }

    @Nonnull
    @Override
    public <T> Ordered<T> putOrdered(@Nonnull String key, long value) {
        return putOrdered(key, getTypedValue(value), false);
    }

    @Override
    public <T> T put(@Nonnull String key, @Nonnull String value) {
        return put(key, getTypedValue(requireNonNull(value)), false);
    }

    @Nonnull
    @Override
    public <T> Ordered<T> putOrdered(@Nonnull String key, @Nonnull String value) {
        return putOrdered(key, getTypedValue(requireNonNull(value)), false);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, @Nonnull byte[] value) {
        return put(key, getTypedValue(requireNonNull(value)), true);
    }

    @Nonnull
    @Override
    public <T> Ordered<T> putIfAbsentOrdered(@Nonnull String key, @Nonnull byte[] value) {
        return putOrdered(key, getTypedValue(requireNonNull(value)), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, int value) {
        return put(key, getTypedValue(value), true);
    }

    @Nonnull
    @Override
    public <T> Ordered<T> putIfAbsentOrdered(@Nonnull String key, int value) {
        return putOrdered(key, getTypedValue(value), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, long value) {
        return put(key, getTypedValue(value), true);
    }

    @Nonnull
    @Override
    public <T> Ordered<T> putIfAbsentOrdered(@Nonnull String key, long value) {
        return putOrdered(key, getTypedValue(value), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, @Nonnull String value) {
        return put(key, getTypedValue(requireNonNull(value)), true);
    }

    @Nonnull
    @Override
    public <T> Ordered<T> putIfAbsentOrdered(@Nonnull String key, @Nonnull String value) {
        return putOrdered(key, getTypedValue(requireNonNull(value)), true);
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
        PutRequest request = PutRequest.newBuilder().setKey(requireNonNull(key)).setValue(value).setAbsent(absent).build();
        PutResponse response = kvStubSupplier.get().put(request).getPutResponse();
        return response.hasValue() ? (T) Serialization.deserialize(response.getValue()) : null;
    }

    private <T> Ordered<T> putOrdered(String key, TypedValue value, boolean absent) {
        PutRequest request = PutRequest.newBuilder().setKey(requireNonNull(key)).setValue(value).setAbsent(absent).build();
        KVResponse response = kvStubSupplier.get().put(request);
        PutResponse putResponse = response.getPutResponse();
        T result = putResponse.hasValue() ? (T) Serialization.deserialize(putResponse.getValue()) : null;
        return new OrderedImpl<>(response.getCommitIndex(), result);
    }

    @Override
    public long setOrdered(String key, @Nonnull byte[] value) {
        return set(key, getTypedValue(requireNonNull(value)));
    }

    @Override
    public long setOrdered(String key, int value) {
        return set(key, getTypedValue(value));
    }

    @Override
    public long setOrdered(String key, long value) {
        return set(key, getTypedValue(value));
    }

    @Override
    public long setOrdered(String key, @Nonnull String value) {
        return set(key, getTypedValue(requireNonNull(value)));
    }

    private long set(String key, TypedValue value) {
        SetRequest request = SetRequest.newBuilder().setKey(requireNonNull(key)).setValue(value).build();
        return kvStubSupplier.get().set(request).getCommitIndex();
    }

    @Override
    public <T> T get(@Nonnull String key, long minCommitIndex) {
        GetRequest request = GetRequest.newBuilder().setKey(requireNonNull(key)).setMinCommitIndex(minCommitIndex).build();
        GetResponse response = kvStubSupplier.get().get(request).getGetResponse();
        return response.hasValue() ? (T) Serialization.deserialize(response.getValue()) : null;
    }

    @Nonnull
    @Override
    public <T> Ordered<T> getOrdered(@Nonnull String key, long minCommitIndex) {
        GetRequest request = GetRequest.newBuilder().setKey(requireNonNull(key)).setMinCommitIndex(minCommitIndex).build();
        KVResponse response = kvStubSupplier.get().get(request);
        GetResponse getResponse = response.getGetResponse();
        T value = getResponse.hasValue() ? (T) Serialization.deserialize(getResponse.getValue()) : null;
        return new OrderedImpl<>(response.getCommitIndex(), value);
    }

    @Nonnull
    @Override
    public <T> Ordered<T> getOrDefaultOrdered(@Nonnull String key, T defaultValue, long minCommitIndex) {
        GetRequest request = GetRequest.newBuilder().setKey(requireNonNull(key)).setMinCommitIndex(minCommitIndex).build();
        KVResponse response = kvStubSupplier.get().get(request);
        GetResponse getResponse = response.getGetResponse();
        T value = getResponse.hasValue() ? (T) Serialization.deserialize(getResponse.getValue()) : defaultValue;
        return new OrderedImpl<>(response.getCommitIndex(), value);
    }

    @Override
    public boolean containsKey(@Nonnull String key, long minCommitIndex) {
        ContainsRequest request = ContainsRequest.newBuilder().setKey(requireNonNull(key)).setMinCommitIndex(minCommitIndex)
                                                 .build();
        ContainsResponse response = kvStubSupplier.get().contains(request).getContainsResponse();
        return response.getSuccess();
    }

    @Nonnull
    @Override
    public Ordered<Boolean> containsKeyOrdered(@Nonnull String key, long minCommitIndex) {
        ContainsRequest request = ContainsRequest.newBuilder().setKey(requireNonNull(key)).setMinCommitIndex(minCommitIndex)
                                                 .build();
        KVResponse response = kvStubSupplier.get().contains(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getContainsResponse().getSuccess());
    }

    @Override
    public boolean contains(@Nonnull String key, @Nonnull byte[] value, long minCommitIndex) {
        return contains(key, getTypedValue(requireNonNull(value)), minCommitIndex);
    }

    @Nonnull
    @Override
    public Ordered<Boolean> containsOrdered(@Nonnull String key, @Nonnull byte[] value, long minCommitIndex) {
        return containsOrdered(key, getTypedValue(requireNonNull(value)), minCommitIndex);
    }

    @Override
    public boolean contains(@Nonnull String key, int value, long minCommitIndex) {
        return contains(key, getTypedValue(value), minCommitIndex);
    }

    @Nonnull
    @Override
    public Ordered<Boolean> containsOrdered(@Nonnull String key, int value, long minCommitIndex) {
        return containsOrdered(key, getTypedValue(requireNonNull(value)), minCommitIndex);
    }

    @Override
    public boolean contains(@Nonnull String key, long value, long minCommitIndex) {
        return contains(key, getTypedValue(value), minCommitIndex);
    }

    @Nonnull
    @Override
    public Ordered<Boolean> containsOrdered(@Nonnull String key, long value, long minCommitIndex) {
        return containsOrdered(key, getTypedValue(requireNonNull(value)), minCommitIndex);
    }

    @Override
    public boolean contains(@Nonnull String key, @Nonnull String value, long minCommitIndex) {
        return contains(key, getTypedValue(value), minCommitIndex);
    }

    @Nonnull
    @Override
    public Ordered<Boolean> containsOrdered(@Nonnull String key, @Nonnull String value, long minCommitIndex) {
        return containsOrdered(key, getTypedValue(requireNonNull(value)), minCommitIndex);
    }

    private boolean contains(String key, TypedValue value, long minCommitIndex) {
        ContainsRequest request = ContainsRequest.newBuilder().setKey(requireNonNull(key)).setValue(value)
                                                 .setMinCommitIndex(minCommitIndex).build();
        return kvStubSupplier.get().contains(request).getContainsResponse().getSuccess();
    }

    private Ordered<Boolean> containsOrdered(String key, TypedValue value, long minCommitIndex) {
        ContainsRequest request = ContainsRequest.newBuilder().setKey(requireNonNull(key)).setValue(value)
                                                 .setMinCommitIndex(minCommitIndex).build();
        KVResponse response = kvStubSupplier.get().contains(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getContainsResponse().getSuccess());
    }

    @Override
    public boolean delete(@Nonnull String key) {
        DeleteRequest request = DeleteRequest.newBuilder().setKey(requireNonNull(key)).build();
        DeleteResponse response = kvStubSupplier.get().delete(request).getDeleteResponse();
        return response.getSuccess();
    }

    @Nonnull
    @Override
    public Ordered<Boolean> deleteOrdered(@Nonnull String key) {
        DeleteRequest request = DeleteRequest.newBuilder().setKey(requireNonNull(key)).build();
        KVResponse response = kvStubSupplier.get().delete(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getDeleteResponse().getSuccess());
    }

    @Override
    public <T> T remove(@Nonnull String key) {
        RemoveRequest request = RemoveRequest.newBuilder().setKey(requireNonNull(key)).build();
        RemoveResponse response = kvStubSupplier.get().remove(request).getRemoveResponse();
        return response.hasValue() ? (T) Serialization.deserialize(response.getValue()) : null;
    }

    @Nonnull
    @Override
    public <T> Ordered<T> removeOrdered(@Nonnull String key) {
        RemoveRequest request = RemoveRequest.newBuilder().setKey(requireNonNull(key)).build();
        KVResponse response = kvStubSupplier.get().remove(request);
        RemoveResponse removeResponse = response.getRemoveResponse();
        T val = removeResponse.hasValue() ? (T) Serialization.deserialize(removeResponse.getValue()) : null;
        return new OrderedImpl<>(response.getCommitIndex(), val);
    }

    @Override
    public boolean remove(@Nonnull String key, byte[] value) {
        return remove(key, getTypedValue(requireNonNull(value)));
    }

    @Nonnull
    @Override
    public Ordered<Boolean> removeOrdered(@Nonnull String key, byte[] value) {
        return removeOrdered(key, getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, int value) {
        return remove(key, getTypedValue(value));
    }

    @Nonnull
    @Override
    public Ordered<Boolean> removeOrdered(@Nonnull String key, int value) {
        return removeOrdered(key, getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, long value) {
        return remove(key, getTypedValue(value));
    }

    @Nonnull
    @Override
    public Ordered<Boolean> removeOrdered(@Nonnull String key, long value) {
        return removeOrdered(key, getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, @Nonnull String value) {
        return remove(key, getTypedValue(value));
    }

    @Nonnull
    @Override
    public Ordered<Boolean> removeOrdered(@Nonnull String key, @Nonnull String value) {
        return removeOrdered(key, getTypedValue(value));
    }

    private boolean remove(String key, TypedValue value) {
        RemoveRequest request = RemoveRequest.newBuilder().setKey(requireNonNull(key)).setValue(value).build();
        return kvStubSupplier.get().remove(request).getRemoveResponse().getSuccess();
    }

    private Ordered<Boolean> removeOrdered(String key, TypedValue value) {
        RemoveRequest request = RemoveRequest.newBuilder().setKey(requireNonNull(key)).setValue(value).build();
        KVResponse response = kvStubSupplier.get().remove(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getRemoveResponse().getSuccess());
    }

    @Override
    public boolean replace(@Nonnull String key, @Nonnull Object oldValue, @Nonnull Object newValue) {
        ReplaceRequest request = ReplaceRequest.newBuilder().setKey(requireNonNull(key))
                                               .setOldValue(getTypedValue(requireNonNull(oldValue)))
                                               .setNewValue(getTypedValue(requireNonNull(newValue))).build();
        ReplaceResponse response = kvStubSupplier.get().replace(request).getReplaceResponse();
        return response.getSuccess();
    }

    @Nonnull
    @Override
    public Ordered<Boolean> replaceOrdered(@Nonnull String key, @Nonnull Object oldValue, @Nonnull Object newValue) {
        ReplaceRequest request = ReplaceRequest.newBuilder().setKey(requireNonNull(key))
                                               .setOldValue(getTypedValue(requireNonNull(oldValue)))
                                               .setNewValue(getTypedValue(requireNonNull(newValue))).build();
        KVResponse response = kvStubSupplier.get().replace(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getReplaceResponse().getSuccess());
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

    @Nonnull
    @Override
    public Ordered<Boolean> isEmptyOrdered(long minCommitIndex) {
        SizeRequest request = SizeRequest.newBuilder().setMinCommitIndex(minCommitIndex).build();
        KVResponse response = kvStubSupplier.get().size(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getSizeResponse().getSize() == 0);
    }

    @Override
    public int size(long minCommitIndex) {
        SizeRequest request = SizeRequest.newBuilder().setMinCommitIndex(minCommitIndex).build();
        SizeResponse response = kvStubSupplier.get().size(request).getSizeResponse();
        return response.getSize();
    }

    @Nonnull
    @Override
    public Ordered<Integer> sizeOrdered(long minCommitIndex) {
        SizeRequest request = SizeRequest.newBuilder().setMinCommitIndex(minCommitIndex).build();
        KVResponse response = kvStubSupplier.get().size(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getSizeResponse().getSize());
    }

    @Override
    public int clear() {
        ClearResponse response = kvStubSupplier.get().clear(ClearRequest.getDefaultInstance()).getClearResponse();
        return response.getSize();
    }

    @Nonnull
    @Override
    public Ordered<Integer> clearOrdered() {
        KVResponse response = kvStubSupplier.get().clear(ClearRequest.getDefaultInstance());
        return new OrderedImpl<>(response.getCommitIndex(), response.getClearResponse().getSize());
    }

    private static class OrderedImpl<T>
            implements Ordered<T> {

        private final long commitIndex;
        private final T value;

        public OrderedImpl(long commitIndex, T value) {
            this.commitIndex = commitIndex;
            this.value = value;
        }

        @Override
        public long getCommitIndex() {
            return commitIndex;
        }

        @Override
        public T get() {
            return value;
        }

    }
}
