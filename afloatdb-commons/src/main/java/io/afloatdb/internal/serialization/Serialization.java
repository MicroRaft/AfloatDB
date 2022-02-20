package io.afloatdb.internal.serialization;

import com.google.protobuf.ByteString;
import io.afloatdb.kv.proto.TypedValue;

import static java.util.Objects.requireNonNull;

import javax.annotation.Nonnull;

public final class Serialization {

    public static final int BYTE_ARRAY_TYPE = 0;
    public static final int INT_TYPE = 1;
    public static final int LONG_TYPE = 2;
    public static final int STRING_TYPE = 3;

    private Serialization() {
    }

    public static ByteString serializeBytes(@Nonnull byte[] b) {
        return ByteString.copyFrom(requireNonNull(b));
    }

    public static ByteString serializeLong(long l) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) (l >>> 56);
        bytes[1] = (byte) (l >>> 48);
        bytes[2] = (byte) (l >>> 40);
        bytes[3] = (byte) (l >>> 32);
        bytes[4] = (byte) (l >>> 24);
        bytes[5] = (byte) (l >>> 16);
        bytes[6] = (byte) (l >>> 8);
        bytes[7] = (byte) (l);

        return ByteString.copyFrom(bytes);
    }

    public static ByteString serializeInt(int i) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((i >>> 24) & 0xFF);
        bytes[1] = (byte) ((i >>> 16) & 0xFF);
        bytes[2] = (byte) ((i >>> 8) & 0xFF);
        bytes[3] = (byte) ((i) & 0xFF);

        return ByteString.copyFrom(bytes);
    }

    public static ByteString serializeString(@Nonnull String s) {
        return ByteString.copyFromUtf8(requireNonNull(s));
    }

    public static Object deserialize(@Nonnull TypedValue typedValue) {
        switch (typedValue.getType()) {
            case BYTE_ARRAY_TYPE:
                return deserializeBytes(typedValue.getValue());
            case INT_TYPE:
                return deserializeInt(typedValue.getValue());
            case LONG_TYPE:
                return deserializeLong(typedValue.getValue());
            case STRING_TYPE:
                return deserializeString(typedValue.getValue());
            default:
                throw new IllegalArgumentException("Invalid typed value: " + typedValue);
        }
    }

    public static byte[] deserializeBytes(@Nonnull ByteString bytes) {
        return bytes.toByteArray();
    }

    public static long deserializeLong(@Nonnull ByteString bytes) {
        long byte7 = (long) bytes.byteAt(0) << 56;
        long byte6 = (long) (bytes.byteAt(1) & 0xFF) << 48;
        long byte5 = (long) (bytes.byteAt(2) & 0xFF) << 40;
        long byte4 = (long) (bytes.byteAt(3) & 0xFF) << 32;
        long byte3 = (long) (bytes.byteAt(4) & 0xFF) << 24;
        long byte2 = (long) (bytes.byteAt(5) & 0xFF) << 16;
        long byte1 = (long) (bytes.byteAt(6) & 0xFF) << 8;
        long byte0 = bytes.byteAt(7) & 0xFF;

        return byte7 | byte6 | byte5 | byte4 | byte3 | byte2 | byte1 | byte0;
    }

    public static int deserializeInt(@Nonnull ByteString bytes) {
        int byte3 = (bytes.byteAt(0) & 0xFF) << 24;
        int byte2 = (bytes.byteAt(1) & 0xFF) << 16;
        int byte1 = (bytes.byteAt(2) & 0xFF) << 8;
        int byte0 = bytes.byteAt(3) & 0xFF;

        return byte3 | byte2 | byte1 | byte0;
    }

    public static String deserializeString(@Nonnull ByteString bytes) {
        return bytes.toStringUtf8();
    }

    public static TypedValue getTypedValue(@Nonnull Object object) {
        if (object == null) {
            throw new NullPointerException();
        }

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

    public static TypedValue getTypedValue(@Nonnull String value) {
        return TypedValue.newBuilder().setType(STRING_TYPE).setValue(serializeString(value)).build();
    }

    public static TypedValue getTypedValue(long value) {
        return TypedValue.newBuilder().setType(LONG_TYPE).setValue(serializeLong(value)).build();
    }

    public static TypedValue getTypedValue(int value) {
        return TypedValue.newBuilder().setType(INT_TYPE).setValue(serializeInt(value)).build();
    }

    public static TypedValue getTypedValue(byte[] value) {
        return TypedValue.newBuilder().setType(BYTE_ARRAY_TYPE).setValue(serializeBytes(value)).build();
    }

}
