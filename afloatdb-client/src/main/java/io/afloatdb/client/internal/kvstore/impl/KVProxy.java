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

import static io.afloatdb.internal.serialization.Serialization.BOOLEAN_TYPE;
import static io.afloatdb.internal.serialization.Serialization.BYTE_ARRAY_TYPE;
import static io.afloatdb.internal.serialization.Serialization.BYTE_TYPE;
import static io.afloatdb.internal.serialization.Serialization.CHAR_TYPE;
import static io.afloatdb.internal.serialization.Serialization.DOUBLE_TYPE;
import static io.afloatdb.internal.serialization.Serialization.FLOAT_TYPE;
import static io.afloatdb.internal.serialization.Serialization.INT_TYPE;
import static io.afloatdb.internal.serialization.Serialization.LONG_TYPE;
import static io.afloatdb.internal.serialization.Serialization.SHORT_TYPE;
import static io.afloatdb.internal.serialization.Serialization.STRING_TYPE;
import static io.afloatdb.internal.serialization.Serialization.serializeBoolean;
import static io.afloatdb.internal.serialization.Serialization.serializeByte;
import static io.afloatdb.internal.serialization.Serialization.serializeBytes;
import static io.afloatdb.internal.serialization.Serialization.serializeChar;
import static io.afloatdb.internal.serialization.Serialization.serializeDouble;
import static io.afloatdb.internal.serialization.Serialization.serializeFloat;
import static io.afloatdb.internal.serialization.Serialization.serializeInt;
import static io.afloatdb.internal.serialization.Serialization.serializeLong;
import static io.afloatdb.internal.serialization.Serialization.serializeShort;
import static io.afloatdb.internal.serialization.Serialization.serializeString;
import static java.util.Objects.requireNonNull;

// TODO [basri] wrap grpc exceptions
public class KVProxy
        implements KV {

    private final KVServiceBlockingStub stub;

    public KVProxy(KVServiceBlockingStub stub) {
        this.stub = stub;
    }

    @Override
    public <T> T put(@Nonnull String key, boolean value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), false);
    }

    @Override
    public <T> T put(@Nonnull String key, byte value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), false);
    }

    @Override
    public <T> T put(@Nonnull String key, @Nonnull byte[] value) {
        requireNonNull(key);
        requireNonNull(value);
        return put(key, getTypedValue(value), false);
    }

    @Override
    public <T> T put(@Nonnull String key, char value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), false);
    }

    @Override
    public <T> T put(@Nonnull String key, double value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), false);
    }

    @Override
    public <T> T put(@Nonnull String key, float value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), false);
    }

    @Override
    public <T> T put(@Nonnull String key, int value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), false);
    }

    @Override
    public <T> T put(@Nonnull String key, long value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), false);
    }

    @Override
    public <T> T put(@Nonnull String key, short value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), false);
    }

    @Override
    public <T> T put(@Nonnull String key, @Nonnull String value) {
        requireNonNull(key);
        requireNonNull(value);
        return put(key, getTypedValue(value), false);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, boolean value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, byte value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, @Nonnull byte[] value) {
        requireNonNull(key);
        requireNonNull(value);
        return put(key, getTypedValue(value), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, char value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, double value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, float value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, int value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, long value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, short value) {
        requireNonNull(key);
        return put(key, getTypedValue(value), true);
    }

    @Override
    public <T> T putIfAbsent(@Nonnull String key, @Nonnull String value) {
        requireNonNull(key);
        requireNonNull(value);
        return put(key, getTypedValue(value), true);
    }

    private TypedValue getTypedValue(@Nonnull String value) {
        return TypedValue.newBuilder().setType(STRING_TYPE).setValue(serializeString(value)).build();
    }

    private TypedValue getTypedValue(short value) {
        return TypedValue.newBuilder().setType(SHORT_TYPE).setValue(serializeShort(value)).build();
    }

    private TypedValue getTypedValue(long value) {
        return TypedValue.newBuilder().setType(LONG_TYPE).setValue(serializeLong(value)).build();
    }

    private TypedValue getTypedValue(int value) {
        return TypedValue.newBuilder().setType(INT_TYPE).setValue(serializeInt(value)).build();
    }

    private TypedValue getTypedValue(float value) {
        return TypedValue.newBuilder().setType(FLOAT_TYPE).setValue(serializeFloat(value)).build();
    }

    private TypedValue getTypedValue(double value) {
        return TypedValue.newBuilder().setType(DOUBLE_TYPE).setValue(serializeDouble(value)).build();
    }

    private TypedValue getTypedValue(char value) {
        return TypedValue.newBuilder().setType(CHAR_TYPE).setValue(serializeChar(value)).build();
    }

    private TypedValue getTypedValue(byte[] value) {
        return TypedValue.newBuilder().setType(BYTE_ARRAY_TYPE).setValue(serializeBytes(value)).build();
    }

    private TypedValue getTypedValue(byte value) {
        return TypedValue.newBuilder().setType(BYTE_TYPE).setValue(serializeByte(value)).build();
    }

    private <T> T put(String key, TypedValue value, boolean absent) {
        PutRequest request = PutRequest.newBuilder().setKey(key).setValue(value).setAbsent(absent).build();
        PutResponse response = stub.put(request);
        return response.hasValue() ? (T) Serialization.deserialize(response.getValue()) : null;
    }

    private TypedValue getTypedValue(boolean value) {
        return TypedValue.newBuilder().setType(BOOLEAN_TYPE).setValue(serializeBoolean(value)).build();
    }

    @Override
    public void set(String key, boolean value) {
        requireNonNull(key);
        set(key, getTypedValue(value));
    }

    @Override
    public void set(String key, byte value) {
        requireNonNull(key);
        set(key, getTypedValue(value));
    }

    @Override
    public void set(String key, @Nonnull byte[] value) {
        requireNonNull(key);
        requireNonNull(value);
        set(key, getTypedValue(value));
    }

    @Override
    public void set(String key, char value) {
        requireNonNull(key);
        set(key, getTypedValue(value));
    }

    @Override
    public void set(String key, double value) {
        requireNonNull(key);
        set(key, getTypedValue(value));
    }

    @Override
    public void set(String key, float value) {
        requireNonNull(key);
        set(key, getTypedValue(value));
    }

    @Override
    public void set(String key, int value) {
        requireNonNull(key);
        set(key, getTypedValue(value));
    }

    @Override
    public void set(String key, long value) {
        requireNonNull(key);
        set(key, getTypedValue(value));
    }

    @Override
    public void set(String key, short value) {
        requireNonNull(key);
        set(key, getTypedValue(value));
    }

    @Override
    public void set(String key, @Nonnull String value) {
        requireNonNull(key);
        requireNonNull(value);
        set(key, getTypedValue(value));
    }

    private void set(String key, TypedValue value) {
        SetRequest request = SetRequest.newBuilder().setKey(key).setValue(value).build();
        stub.set(request);
    }

    @Override
    public <T> T get(String key) {
        requireNonNull(key);

        GetRequest request = GetRequest.newBuilder().setKey(key).build();
        GetResponse response = stub.get(request);

        return response.hasValue() ? (T) Serialization.deserialize(response.getValue()) : null;
    }

    @Override
    public boolean contains(@Nonnull String key) {
        ContainsRequest request = ContainsRequest.newBuilder().setKey(key).build();

        ContainsResponse response = stub.contains(request);

        return response.getSuccess();
    }

    @Override
    public boolean contains(@Nonnull String key, boolean value) {
        requireNonNull(key);
        return contains(key, getTypedValue(value));
    }

    @Override
    public boolean contains(@Nonnull String key, byte value) {
        requireNonNull(key);
        return contains(key, getTypedValue(value));
    }

    @Override
    public boolean contains(@Nonnull String key, @Nonnull byte[] value) {
        requireNonNull(key);
        requireNonNull(value);
        return contains(key, getTypedValue(value));
    }

    @Override
    public boolean contains(@Nonnull String key, char value) {
        requireNonNull(key);
        return contains(key, getTypedValue(value));
    }

    @Override
    public boolean contains(@Nonnull String key, double value) {
        requireNonNull(key);
        return contains(key, getTypedValue(value));
    }

    @Override
    public boolean contains(@Nonnull String key, float value) {
        requireNonNull(key);
        return contains(key, getTypedValue(value));
    }

    @Override
    public boolean contains(@Nonnull String key, int value) {
        requireNonNull(key);
        return contains(key, getTypedValue(value));
    }

    @Override
    public boolean contains(@Nonnull String key, long value) {
        requireNonNull(key);
        return contains(key, getTypedValue(value));
    }

    @Override
    public boolean contains(@Nonnull String key, short value) {
        requireNonNull(key);
        return contains(key, getTypedValue(value));
    }

    @Override
    public boolean contains(@Nonnull String key, @Nonnull String value) {
        requireNonNull(key);
        return contains(key, getTypedValue(value));
    }

    private boolean contains(String key, TypedValue value) {
        ContainsRequest request = ContainsRequest.newBuilder().setKey(key).setValue(value).build();

        return stub.contains(request).getSuccess();
    }

    @Override
    public boolean delete(@Nonnull String key) {
        requireNonNull(key);

        DeleteRequest request = DeleteRequest.newBuilder().setKey(key).build();

        DeleteResponse response = stub.delete(request);

        return response.getSuccess();
    }

    @Override
    public <T> T remove(@Nonnull String key) {
        requireNonNull(key);

        RemoveRequest request = RemoveRequest.newBuilder().setKey(key).build();

        RemoveResponse response = stub.remove(request);

        return response.hasValue() ? (T) Serialization.deserialize(response.getValue()) : null;
    }

    @Override
    public boolean remove(@Nonnull String key, boolean value) {
        requireNonNull(key);
        return remove(key, getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, byte value) {
        requireNonNull(key);
        return remove(key, getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, byte[] value) {
        requireNonNull(key);
        requireNonNull(value);
        return remove(key, getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, char value) {
        requireNonNull(key);
        return remove(key, getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, double value) {
        requireNonNull(key);
        return remove(key, getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, float value) {
        requireNonNull(key);
        return remove(key, getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, int value) {
        requireNonNull(key);
        return remove(key, getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, long value) {
        requireNonNull(key);
        return remove(key, getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, short value) {
        requireNonNull(key);
        return remove(key, getTypedValue(value));
    }

    @Override
    public boolean remove(@Nonnull String key, @Nonnull String value) {
        requireNonNull(key);
        return remove(key, getTypedValue(value));
    }

    private boolean remove(String key, TypedValue value) {
        RemoveRequest request = RemoveRequest.newBuilder().setKey(key).setValue(value).build();

        return stub.remove(request).getSuccess();
    }

    @Override
    public boolean replace(@Nonnull String key, @Nonnull Object oldValue, @Nonnull Object newValue) {
        requireNonNull(key);
        requireNonNull(oldValue);
        requireNonNull(newValue);

        ReplaceRequest request = ReplaceRequest.newBuilder().setKey(key).setOldValue(getTypedValue(oldValue))
                                               .setNewValue(getTypedValue(newValue)).build();

        ReplaceResponse response = stub.replace(request);

        return response.getSuccess();
    }

    private TypedValue getTypedValue(Object object) {
        if (object instanceof Boolean) {
            return getTypedValue((boolean) object);
        } else if (object instanceof Byte) {
            return getTypedValue((byte) object);
        } else if (object instanceof byte[]) {
            return getTypedValue((byte[]) object);
        } else if (object instanceof Character) {
            return getTypedValue((char) object);
        } else if (object instanceof Double) {
            return getTypedValue((double) object);
        } else if (object instanceof Float) {
            return getTypedValue((float) object);
        } else if (object instanceof Integer) {
            return getTypedValue((int) object);
        } else if (object instanceof Long) {
            return getTypedValue((long) object);
        } else if (object instanceof Short) {
            return getTypedValue((short) object);
        } else if (object instanceof String) {
            return getTypedValue((String) object);
        }

        throw new IllegalArgumentException(object + " has invalid type!");
    }

    @Override
    public int size() {
        SizeResponse response = stub.size(SizeRequest.getDefaultInstance());

        return response.getSize();
    }

    @Override
    public int clear() {
        ClearResponse response = stub.clear(ClearRequest.getDefaultInstance());

        return response.getSize();
    }

}
