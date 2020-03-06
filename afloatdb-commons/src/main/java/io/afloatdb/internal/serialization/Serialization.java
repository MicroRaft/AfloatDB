package io.afloatdb.internal.serialization;

import com.google.protobuf.ByteString;
import io.afloatdb.kv.proto.TypedValue;

import static java.util.Objects.requireNonNull;

public final class Serialization {

    public static final int BOOLEAN_TYPE = 0;
    public static final int BYTE_TYPE = 1;
    public static final int BYTE_ARRAY_TYPE = 2;
    public static final int CHAR_TYPE = 3;
    public static final int DOUBLE_TYPE = 4;
    public static final int FLOAT_TYPE = 5;
    public static final int INT_TYPE = 6;
    public static final int LONG_TYPE = 7;
    public static final int SHORT_TYPE = 8;
    public static final int STRING_TYPE = 9;

    private static final ByteString TRUE_BYTE_STRING = ByteString.copyFrom(new byte[]{1});
    private static final ByteString FALSE_BYTE_STRING = ByteString.copyFrom(new byte[]{0});

    private Serialization() {
    }

    public static ByteString serializeBoolean(boolean b) {
        return b ? TRUE_BYTE_STRING : FALSE_BYTE_STRING;
    }

    public static ByteString serializeByte(byte b) {
        return ByteString.copyFrom(new byte[]{b});
    }

    public static ByteString serializeBytes(byte[] b) {
        requireNonNull(b);
        return ByteString.copyFrom(b);
    }

    public static ByteString serializeChar(char ch) {
        return ByteString.copyFrom(new byte[]{(byte) ((ch >>> 8) & 0xFF), (byte) (ch & 0xFF)});
    }

    public static ByteString serializeDouble(double d) {
        return serializeLong(Double.doubleToRawLongBits(d));
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

    public static ByteString serializeFloat(float f) {
        return serializeInt(Float.floatToRawIntBits(f));
    }

    public static ByteString serializeInt(int i) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((i >>> 24) & 0xFF);
        bytes[1] = (byte) ((i >>> 16) & 0xFF);
        bytes[2] = (byte) ((i >>> 8) & 0xFF);
        bytes[3] = (byte) ((i) & 0xFF);

        return ByteString.copyFrom(bytes);
    }

    public static ByteString serializeShort(short s) {
        byte[] bytes = new byte[2];
        bytes[0] = (byte) ((s >>> 8) & 0xFF);
        bytes[1] = (byte) ((s) & 0xFF);

        return ByteString.copyFrom(bytes);
    }

    public static ByteString serializeString(String s) {
        return ByteString.copyFromUtf8(s);
    }

    public static Object deserialize(TypedValue typedValue) {
        requireNonNull(typedValue);

        switch (typedValue.getType()) {
            case BOOLEAN_TYPE:
                return deserializeBoolean(typedValue.getValue());
            case BYTE_TYPE:
                return deserializeByte(typedValue.getValue());
            case BYTE_ARRAY_TYPE:
                return deserializeBytes(typedValue.getValue());
            case CHAR_TYPE:
                return deserializeChar(typedValue.getValue());
            case DOUBLE_TYPE:
                return deserializeDouble(typedValue.getValue());
            case FLOAT_TYPE:
                return deserializeFloat(typedValue.getValue());
            case INT_TYPE:
                return deserializeInt(typedValue.getValue());
            case LONG_TYPE:
                return deserializeLong(typedValue.getValue());
            case SHORT_TYPE:
                return deserializeShort(typedValue.getValue());
            case STRING_TYPE:
                return deserializeString(typedValue.getValue());
            default:
                throw new IllegalArgumentException("Invalid typed value: " + typedValue);
        }
    }

    public static boolean deserializeBoolean(ByteString bytes) {
        return bytes.byteAt(0) == 1 ? Boolean.TRUE : Boolean.FALSE;
    }

    public static byte deserializeByte(ByteString bytes) {
        return bytes.byteAt(0);
    }

    public static byte[] deserializeBytes(ByteString bytes) {
        requireNonNull(bytes);
        return bytes.toByteArray();
    }

    public static char deserializeChar(ByteString bytes) {
        int byte1 = (bytes.byteAt(0) & 0xFF) << 8;
        int byte0 = bytes.byteAt(1) & 0xFF;

        return (char) (byte1 | byte0);
    }

    public static double deserializeDouble(ByteString bytes) {
        return Double.longBitsToDouble(deserializeLong(bytes));
    }

    public static long deserializeLong(ByteString bytes) {
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

    public static float deserializeFloat(ByteString bytes) {
        return Float.intBitsToFloat(deserializeInt(bytes));
    }

    public static int deserializeInt(ByteString bytes) {
        int byte3 = (bytes.byteAt(0) & 0xFF) << 24;
        int byte2 = (bytes.byteAt(1) & 0xFF) << 16;
        int byte1 = (bytes.byteAt(2) & 0xFF) << 8;
        int byte0 = bytes.byteAt(3) & 0xFF;

        return byte3 | byte2 | byte1 | byte0;
    }

    public static short deserializeShort(ByteString bytes) {
        int byte1 = bytes.byteAt(0) & 0xFF;
        int byte0 = bytes.byteAt(1) & 0xFF;
        return (short) ((byte1 << 8) | byte0);
    }

    public static String deserializeString(ByteString bytes) {
        return bytes.toStringUtf8();
    }

}
