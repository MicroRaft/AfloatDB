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
import io.afloatdb.kv.proto.ContainsRequest;
import io.afloatdb.kv.proto.DeleteRequest;
import io.afloatdb.kv.proto.GetRequest;
import io.afloatdb.kv.proto.GetResponse;
import io.afloatdb.kv.proto.KVRequestHandlerGrpc.KVRequestHandlerBlockingStub;
import io.afloatdb.kv.proto.KVResponse;
import io.afloatdb.kv.proto.PutRequest;
import io.afloatdb.kv.proto.PutResponse;
import io.afloatdb.kv.proto.RemoveRequest;
import io.afloatdb.kv.proto.RemoveResponse;
import io.afloatdb.kv.proto.ReplaceRequest;
import io.afloatdb.kv.proto.SetRequest;
import io.afloatdb.kv.proto.SizeRequest;
import io.afloatdb.kv.proto.TypedValue;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

import static io.afloatdb.internal.serialization.Serialization.getTypedValue;
import static java.util.Objects.requireNonNull;

public class KVProxy implements KV {

    private final Supplier<KVRequestHandlerBlockingStub> kvStubSupplier;

    public KVProxy(@Nonnull Supplier<KVRequestHandlerBlockingStub> kvStubSupplier) {
        this.kvStubSupplier = requireNonNull(kvStubSupplier);
    }

    @Nonnull
    @Override
    public Ordered<byte[]> put(@Nonnull String key, @Nonnull byte[] value) {
        return putOrdered(key, getTypedValue(value), false);
    }

    @Nonnull
    @Override
    public Ordered<Integer> put(@Nonnull String key, int value) {
        return putOrdered(key, getTypedValue(value), false);
    }

    @Nonnull
    @Override
    public Ordered<Long> put(@Nonnull String key, long value) {
        return putOrdered(key, getTypedValue(value), false);
    }

    @Nonnull
    @Override
    public Ordered<String> put(@Nonnull String key, @Nonnull String value) {
        return putOrdered(key, getTypedValue(value), false);
    }

    @Nonnull
    @Override
    public Ordered<byte[]> putIfAbsent(@Nonnull String key, @Nonnull byte[] value) {
        return putOrdered(key, getTypedValue(value), true);
    }

    @Nonnull
    @Override
    public Ordered<Integer> putIfAbsent(@Nonnull String key, int value) {
        return putOrdered(key, getTypedValue(value), true);
    }

    @Nonnull
    @Override
    public Ordered<Long> putIfAbsent(@Nonnull String key, long value) {
        return putOrdered(key, getTypedValue(value), true);
    }

    @Nonnull
    @Override
    public Ordered<String> putIfAbsent(@Nonnull String key, @Nonnull String value) {
        return putOrdered(key, getTypedValue(value), true);
    }

    private <T> Ordered<T> putOrdered(String key, TypedValue value, boolean absent) {
        PutRequest request = PutRequest.newBuilder().setKey(requireNonNull(key)).setValue(value).setAbsent(absent)
                .build();
        KVResponse response = kvStubSupplier.get().put(request);
        PutResponse putResponse = response.getPutResponse();
        T result = putResponse.hasValue() ? (T) Serialization.deserialize(putResponse.getValue()) : null;
        return new OrderedImpl<>(response.getCommitIndex(), result);
    }

    @Override
    public Ordered<Void> set(@Nonnull String key, @Nonnull byte[] value) {
        return set(key, getTypedValue(value));
    }

    @Override
    public Ordered<Void> set(@Nonnull String key, int value) {
        return set(key, getTypedValue(value));
    }

    @Override
    public Ordered<Void> set(@Nonnull String key, long value) {
        return set(key, getTypedValue(value));
    }

    @Override
    public Ordered<Void> set(@Nonnull String key, @Nonnull String value) {
        return set(key, getTypedValue(value));
    }

    private Ordered<Void> set(@Nonnull String key, @Nonnull TypedValue value) {
        SetRequest request = SetRequest.newBuilder().setKey(requireNonNull(key)).setValue(value).build();
        KVResponse response = kvStubSupplier.get().set(request);
        return new OrderedImpl<>(response.getCommitIndex(), null);
    }

    @Nonnull
    @Override
    public <T> Ordered<T> get(@Nonnull String key, long minCommitIndex) {
        GetRequest request = GetRequest.newBuilder().setKey(requireNonNull(key)).setMinCommitIndex(minCommitIndex)
                .build();
        KVResponse response = kvStubSupplier.get().get(request);
        GetResponse getResponse = response.getGetResponse();
        T value = getResponse.hasValue() ? (T) Serialization.deserialize(getResponse.getValue()) : null;
        return new OrderedImpl<>(response.getCommitIndex(), value);
    }

    @Nonnull
    @Override
    public Ordered<Boolean> containsKey(@Nonnull String key, long minCommitIndex) {
        ContainsRequest request = ContainsRequest.newBuilder().setKey(requireNonNull(key))
                .setMinCommitIndex(minCommitIndex).build();
        KVResponse response = kvStubSupplier.get().contains(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getContainsResponse().getSuccess());
    }

    @Nonnull
    @Override
    public Ordered<Boolean> contains(@Nonnull String key, @Nonnull byte[] value, long minCommitIndex) {
        return contains(key, getTypedValue(value), minCommitIndex);
    }

    @Nonnull
    @Override
    public Ordered<Boolean> contains(@Nonnull String key, int value, long minCommitIndex) {
        return contains(key, getTypedValue(value), minCommitIndex);
    }

    @Nonnull
    @Override
    public Ordered<Boolean> contains(@Nonnull String key, long value, long minCommitIndex) {
        return contains(key, getTypedValue(value), minCommitIndex);
    }

    @Nonnull
    @Override
    public Ordered<Boolean> contains(@Nonnull String key, @Nonnull String value, long minCommitIndex) {
        return contains(key, getTypedValue(value), minCommitIndex);
    }

    private Ordered<Boolean> contains(@Nonnull String key, @Nonnull TypedValue value, long minCommitIndex) {
        ContainsRequest request = ContainsRequest.newBuilder().setKey(requireNonNull(key)).setValue(value)
                .setMinCommitIndex(minCommitIndex).build();
        KVResponse response = kvStubSupplier.get().contains(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getContainsResponse().getSuccess());
    }

    @Nonnull
    @Override
    public Ordered<Boolean> delete(@Nonnull String key) {
        DeleteRequest request = DeleteRequest.newBuilder().setKey(requireNonNull(key)).build();
        KVResponse response = kvStubSupplier.get().delete(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getDeleteResponse().getSuccess());
    }

    @Nonnull
    @Override
    public <T> Ordered<T> remove(@Nonnull String key) {
        RemoveRequest request = RemoveRequest.newBuilder().setKey(requireNonNull(key)).build();
        KVResponse response = kvStubSupplier.get().remove(request);
        RemoveResponse removeResponse = response.getRemoveResponse();
        T val = removeResponse.hasValue() ? (T) Serialization.deserialize(removeResponse.getValue()) : null;
        return new OrderedImpl<>(response.getCommitIndex(), val);
    }

    @Override
    public Ordered<Boolean> remove(@Nonnull String key, @Nonnull byte[] value) {
        return remove(key, getTypedValue(value));
    }

    @Nonnull
    @Override
    public Ordered<Boolean> remove(@Nonnull String key, int value) {
        return remove(key, getTypedValue(value));
    }

    @Nonnull
    @Override
    public Ordered<Boolean> remove(@Nonnull String key, long value) {
        return remove(key, getTypedValue(value));
    }

    @Nonnull
    @Override
    public Ordered<Boolean> remove(@Nonnull String key, @Nonnull String value) {
        return remove(key, getTypedValue(value));
    }

    private Ordered<Boolean> remove(@Nonnull String key, @Nonnull TypedValue value) {
        RemoveRequest request = RemoveRequest.newBuilder().setKey(requireNonNull(key)).setValue(value).build();
        KVResponse response = kvStubSupplier.get().remove(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getRemoveResponse().getSuccess());
    }

    @Nonnull
    @Override
    public Ordered<Boolean> replace(@Nonnull String key, @Nonnull Object oldValue, @Nonnull Object newValue) {
        ReplaceRequest request = ReplaceRequest.newBuilder().setKey(requireNonNull(key))
                .setOldValue(getTypedValue(oldValue))
                .setNewValue(getTypedValue(newValue)).build();
        KVResponse response = kvStubSupplier.get().replace(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getReplaceResponse().getSuccess());
    }

    @Nonnull
    @Override
    public Ordered<Boolean> isEmpty(long minCommitIndex) {
        SizeRequest request = SizeRequest.newBuilder().setMinCommitIndex(minCommitIndex).build();
        KVResponse response = kvStubSupplier.get().size(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getSizeResponse().getSize() == 0);
    }

    @Nonnull
    @Override
    public Ordered<Integer> size(long minCommitIndex) {
        SizeRequest request = SizeRequest.newBuilder().setMinCommitIndex(minCommitIndex).build();
        KVResponse response = kvStubSupplier.get().size(request);
        return new OrderedImpl<>(response.getCommitIndex(), response.getSizeResponse().getSize());
    }

    @Nonnull
    @Override
    public Ordered<Integer> clear() {
        KVResponse response = kvStubSupplier.get().clear(ClearRequest.getDefaultInstance());
        return new OrderedImpl<>(response.getCommitIndex(), response.getClearResponse().getSize());
    }

    private static class OrderedImpl<T> implements Ordered<T> {

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
