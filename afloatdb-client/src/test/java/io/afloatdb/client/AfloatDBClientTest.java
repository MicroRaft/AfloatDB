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

package io.afloatdb.client;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.afloatdb.AfloatDB;
import io.afloatdb.client.config.AfloatDBClientConfig;
import io.afloatdb.client.kvstore.KV;
import io.microraft.impl.util.BaseTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.typesafe.config.ConfigFactory.load;
import static io.afloatdb.utils.AfloatDBTestUtils.CONFIG_1;
import static io.afloatdb.utils.AfloatDBTestUtils.CONFIG_2;
import static io.afloatdb.utils.AfloatDBTestUtils.CONFIG_3;
import static io.afloatdb.utils.AfloatDBTestUtils.waitUntilLeaderElected;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AfloatDBClientTest
        extends BaseTest {

    private static final byte BYTE_1 = 76;
    private static final byte BYTE_2 = 67;
    private static final byte[] BYTES_1 = new byte[]{1, 2, 3, 4};
    private static final byte[] BYTES_2 = new byte[]{4, 3, 2, 1};
    private static final char CHAR_1 = 'k';
    private static final char CHAR_2 = 'j';
    private static final double DOUBLE_1 = 306.23f;
    private static final double DOUBLE_2 = 23781.91f;
    private static final float FLOAT_1 = 8523.23f;
    private static final float FLOAT_2 = 2958892.1f;
    private static final int INT_1 = 753;
    private static final int INT_2 = 1239;
    private static final long LONG_1 = 19238;
    private static final long LONG_2 = 4693;
    private static final short SHORT_1 = 25;
    private static final short SHORT_2 = 75;
    private static final String STRING_1 = "str1";
    private static final String STRING_2 = "str2";
    private static final String KEY = "key";

    private static List<AfloatDB> servers = new ArrayList<>();
    private static AfloatDBClient client;
    private static KV kv;

    @BeforeClass
    public static void init() {
        servers.add(AfloatDB.bootstrap(CONFIG_1));
        servers.add(AfloatDB.bootstrap(CONFIG_2));
        servers.add(AfloatDB.bootstrap(CONFIG_3));

        String leaderAddress = waitUntilLeaderElected(servers).getConfig().getLocalEndpointConfig().getAddress();
        Config config = ConfigFactory.parseString("afloatdb.client.server-address: \"" + leaderAddress + "\"")
                                     .withFallback(load("client.conf"));
        client = AfloatDBClient.newInstance(AfloatDBClientConfig.from(config));
        kv = client.getKV();
    }

    @AfterClass
    public static void tearDown() {
        servers.forEach(AfloatDB::shutdown);
        if (client != null) {
            client.shutdown();
        }
    }

    @Before
    public void beforeTest() {
        kv.clear();
    }

    @Test(expected = NullPointerException.class)
    public void testPutNullKey() {
        kv.put(null, "val1");
    }

    @Test
    public void testPutBoolean() {
        Boolean bool = kv.put(KEY, false);
        assertThat(bool).isNull();

        bool = kv.put(KEY, true);
        assertThat(bool).isFalse();

        bool = kv.get(KEY);
        assertThat(bool).isTrue();
    }

    @Test
    public void testPutByte() {
        Byte b = kv.put(KEY, BYTE_1);
        assertThat(b).isNull();

        b = kv.put(KEY, BYTE_2);
        assertThat(b).isEqualTo(BYTE_1);

        b = kv.get(KEY);
        assertThat(b).isEqualTo(BYTE_2);
    }

    @Test
    public void testPutByteArray() {
        byte[] bytes = kv.put(KEY, BYTES_1);
        assertThat(bytes).isNull();

        bytes = kv.put(KEY, BYTES_2);
        assertThat(bytes).isEqualTo(BYTES_1);

        bytes = kv.get(KEY);
        assertThat(bytes).isEqualTo(BYTES_2);
    }

    @Test
    public void testPutChar() {
        Character ch = kv.put(KEY, CHAR_1);
        assertThat(ch).isNull();

        ch = kv.put(KEY, CHAR_2);
        assertThat(ch).isEqualTo(CHAR_1);

        ch = kv.get(KEY);
        assertThat(ch).isEqualTo(CHAR_2);
    }

    @Test
    public void testPutDouble() {
        Double d = kv.put(KEY, DOUBLE_1);
        assertThat(d).isNull();

        d = kv.put(KEY, DOUBLE_2);
        assertThat(d).isEqualTo(DOUBLE_1);

        d = kv.get(KEY);
        assertThat(d).isEqualTo(DOUBLE_2);
    }

    @Test
    public void testPutFloat() {
        Float f = kv.put(KEY, FLOAT_1);
        assertThat(f).isNull();

        f = kv.put(KEY, FLOAT_2);
        assertThat(f).isEqualTo(FLOAT_1);

        f = kv.get(KEY);
        assertThat(f).isEqualTo(FLOAT_2);
    }

    @Test
    public void testPutInt() {
        Integer i = kv.put(KEY, INT_1);
        assertThat(i).isNull();

        i = kv.put(KEY, INT_2);
        assertThat(i).isEqualTo(INT_1);

        i = kv.get(KEY);
        assertThat(i).isEqualTo(INT_2);
    }

    @Test
    public void testPutLong() {
        Long l = kv.put(KEY, LONG_1);
        assertThat(l).isNull();

        l = kv.put(KEY, LONG_2);
        assertThat(l).isEqualTo(LONG_1);

        l = kv.get(KEY);
        assertThat(l).isEqualTo(LONG_2);
    }

    @Test
    public void testPutShort() {
        Short s = kv.put(KEY, SHORT_1);
        assertThat(s).isNull();

        s = kv.put(KEY, SHORT_2);
        assertThat(s).isEqualTo(SHORT_1);

        s = kv.get(KEY);
        assertThat(s).isEqualTo(SHORT_2);
    }

    @Test
    public void testPutString() {
        String s = kv.put(KEY, STRING_1);
        assertThat(s).isNull();

        s = kv.put(KEY, STRING_2);
        assertThat(s).isEqualTo(STRING_1);

        s = kv.get(KEY);
        assertThat(s).isEqualTo(STRING_2);
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsentNullKey() {
        kv.putIfAbsent(null, "val1");
    }

    @Test
    public void testPutBooleanIfAbsent() {
        Boolean bool = kv.putIfAbsent(KEY, false);
        assertThat(bool).isNull();

        bool = kv.get(KEY);
        assertThat(bool).isFalse();

        bool = kv.putIfAbsent(KEY, true);
        assertThat(bool).isFalse();
    }

    @Test
    public void testPutByteIfAbsent() {
        Byte b = kv.putIfAbsent(KEY, BYTE_1);
        assertThat(b).isNull();

        b = kv.get(KEY);
        assertThat(b).isEqualTo(BYTE_1);

        b = kv.putIfAbsent(KEY, BYTE_2);
        assertThat(b).isEqualTo(BYTE_1);
    }

    @Test
    public void testPutByteArrayIfAbsent() {
        byte[] bytes = kv.putIfAbsent(KEY, BYTES_1);
        assertThat(bytes).isNull();

        bytes = kv.get(KEY);
        assertThat(bytes).isEqualTo(BYTES_1);

        bytes = kv.putIfAbsent(KEY, BYTES_2);
        assertThat(bytes).isEqualTo(BYTES_1);
    }

    @Test
    public void testPutCharIfAbsent() {
        Character ch = kv.putIfAbsent(KEY, CHAR_1);
        assertThat(ch).isNull();

        ch = kv.get(KEY);
        assertThat(ch).isEqualTo(CHAR_1);

        ch = kv.putIfAbsent(KEY, CHAR_2);
        assertThat(ch).isEqualTo(CHAR_1);
    }

    @Test
    public void testPutDoubleIfAbsent() {
        Double d = kv.putIfAbsent(KEY, DOUBLE_1);
        assertThat(d).isNull();

        d = kv.get(KEY);
        assertThat(d).isEqualTo(DOUBLE_1);

        d = kv.putIfAbsent(KEY, DOUBLE_2);
        assertThat(d).isEqualTo(DOUBLE_1);
    }

    @Test
    public void testPutFloatIfAbsent() {
        Float f = kv.putIfAbsent(KEY, FLOAT_1);
        assertThat(f).isNull();

        f = kv.get(KEY);
        assertThat(f).isEqualTo(FLOAT_1);

        f = kv.putIfAbsent(KEY, FLOAT_2);
        assertThat(f).isEqualTo(FLOAT_1);
    }

    @Test
    public void testPutIntIfAbsent() {
        Integer i = kv.putIfAbsent(KEY, INT_1);
        assertThat(i).isNull();

        i = kv.get(KEY);
        assertThat(i).isEqualTo(INT_1);

        i = kv.putIfAbsent(KEY, INT_2);
        assertThat(i).isEqualTo(INT_1);
    }

    @Test
    public void testPutLongIfAbsent() {
        Long l = kv.putIfAbsent(KEY, LONG_1);
        assertThat(l).isNull();

        l = kv.get(KEY);
        assertThat(l).isEqualTo(LONG_1);

        l = kv.putIfAbsent(KEY, LONG_2);
        assertThat(l).isEqualTo(LONG_1);
    }

    @Test
    public void testPutShortIfAbsent() {
        Short s = kv.putIfAbsent(KEY, SHORT_1);
        assertThat(s).isNull();

        s = kv.get(KEY);
        assertThat(s).isEqualTo(SHORT_1);

        s = kv.putIfAbsent(KEY, SHORT_2);
        assertThat(s).isEqualTo(SHORT_1);
    }

    @Test
    public void testPutStringIfAbsent() {
        String s = kv.putIfAbsent(KEY, STRING_1);
        assertThat(s).isNull();

        s = kv.get(KEY);
        assertThat(s).isEqualTo(STRING_1);

        s = kv.putIfAbsent(KEY, STRING_2);
        assertThat(s).isEqualTo(STRING_1);
    }

    @Test(expected = NullPointerException.class)
    public void testSetNullKey() {
        kv.set(null, "val1");
    }

    @Test
    public void testSetBoolean() {
        kv.set(KEY, true);

        boolean b = kv.get(KEY);
        assertThat(b).isTrue();
    }

    @Test
    public void testSetByte() {
        kv.set(KEY, BYTE_1);

        byte b = kv.get(KEY);
        assertThat(b).isEqualTo(BYTE_1);
    }

    @Test
    public void testSetByteArray() {
        kv.set(KEY, BYTES_1);

        byte[] b = kv.get(KEY);
        assertThat(b).isEqualTo(BYTES_1);
    }

    @Test
    public void testSetChar() {
        kv.set(KEY, CHAR_1);

        char ch = kv.get(KEY);
        assertThat(ch).isEqualTo(CHAR_1);
    }

    @Test
    public void testSetDouble() {
        kv.set(KEY, DOUBLE_1);

        double d = kv.get(KEY);
        assertThat(d).isEqualTo(DOUBLE_1);
    }

    @Test
    public void testSetFloat() {
        kv.set(KEY, FLOAT_1);

        float f = kv.get(KEY);
        assertThat(f).isEqualTo(FLOAT_1);
    }

    @Test
    public void testSetInt() {
        kv.set(KEY, INT_1);

        int i = kv.get(KEY);
        assertThat(i).isEqualTo(INT_1);
    }

    @Test
    public void testSetLong() {
        kv.set(KEY, LONG_1);

        long l = kv.get(KEY);
        assertThat(l).isEqualTo(LONG_1);
    }

    @Test
    public void testSetShort() {
        kv.set(KEY, SHORT_1);

        short s = kv.get(KEY);
        assertThat(s).isEqualTo(SHORT_1);
    }

    @Test
    public void testSetString() {
        kv.set(KEY, STRING_1);

        String s = kv.get(KEY);
        assertThat(s).isEqualTo(STRING_1);
    }

    @Test
    public void testSet() {
        String key = "key1";
        String val1 = "val1";
        String val2 = "val2";

        String val = kv.get(key);
        assertThat(val).isNull();

        boolean contains = kv.contains(key);
        assertFalse(contains);

        kv.set(key, val1);

        val = kv.get(key);
        assertThat(val).isEqualTo(val1);

        kv.set(key, val2);

        val = kv.get(key);
        assertThat(val).isEqualTo(val2);
    }

    @Test
    public void testGetBooleanOrDefault() {
        boolean b = kv.getOrDefault(KEY, true);
        assertThat(b).isTrue();
    }

    @Test
    public void testGetByteOrDefault() {
        byte b = kv.getOrDefault(KEY, BYTE_2);
        assertThat(b).isEqualTo(BYTE_2);
    }

    @Test
    public void testGetByteArrayOrDefault() {
        byte[] b = kv.getOrDefault(KEY, BYTES_2);
        assertThat(b).isEqualTo(BYTES_2);
    }

    @Test
    public void testGetCharOrDefault() {
        char ch = kv.getOrDefault(KEY, CHAR_2);
        assertThat(ch).isEqualTo(CHAR_2);
    }

    @Test
    public void testGetDoubleOrDefault() {
        double d = kv.getOrDefault(KEY, DOUBLE_2);
        assertThat(d).isEqualTo(DOUBLE_2);
    }

    @Test
    public void testGetFloatOrDefault() {
        float f = kv.getOrDefault(KEY, FLOAT_2);
        assertThat(f).isEqualTo(FLOAT_2);
    }

    @Test
    public void testGetIntOrDefault() {
        int i = kv.getOrDefault(KEY, INT_2);
        assertThat(i).isEqualTo(INT_2);
    }

    @Test
    public void testGetLongOrDefault() {
        long l = kv.getOrDefault(KEY, LONG_2);
        assertThat(l).isEqualTo(LONG_2);
    }

    @Test
    public void testGetShortOrDefault() {
        short s = kv.getOrDefault(KEY, SHORT_2);
        assertThat(s).isEqualTo(SHORT_2);
    }

    @Test
    public void testGetStringOrDefault() {
        String s = kv.getOrDefault(KEY, STRING_2);
        assertThat(s).isEqualTo(STRING_2);
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteNullKey() {
        kv.delete(null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsNullKey() {
        kv.contains(null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsNullKeyForKV() {
        kv.contains(null, STRING_1);
    }

    @Test
    public void testContainsBoolean() {
        kv.set(KEY, true);

        assertThat(kv.contains(KEY)).isTrue();
        assertThat(kv.contains(KEY, true)).isTrue();
        assertThat(kv.contains(KEY, false)).isFalse();

        kv.delete(KEY);
        assertThat(kv.contains(KEY)).isFalse();
    }

    @Test
    public void testContainsByte() {
        kv.set(KEY, BYTE_1);

        assertThat(kv.contains(KEY)).isTrue();
        assertThat(kv.contains(KEY, BYTE_1)).isTrue();
        assertThat(kv.contains(KEY, BYTE_2)).isFalse();

        kv.delete(KEY);
        assertThat(kv.contains(KEY)).isFalse();
    }

    @Test
    public void testContainsByteArray() {
        kv.set(KEY, BYTES_1);

        assertThat(kv.contains(KEY)).isTrue();
        assertThat(kv.contains(KEY, BYTES_1)).isTrue();
        assertThat(kv.contains(KEY, BYTES_2)).isFalse();

        kv.delete(KEY);
        assertThat(kv.contains(KEY)).isFalse();
    }

    @Test
    public void testContainsChar() {
        kv.set(KEY, CHAR_1);

        assertThat(kv.contains(KEY)).isTrue();
        assertThat(kv.contains(KEY, CHAR_1)).isTrue();
        assertThat(kv.contains(KEY, CHAR_2)).isFalse();

        kv.delete(KEY);
        assertThat(kv.contains(KEY)).isFalse();
    }

    @Test
    public void testContainsDouble() {
        kv.set(KEY, DOUBLE_1);

        assertThat(kv.contains(KEY)).isTrue();
        assertThat(kv.contains(KEY, DOUBLE_1)).isTrue();
        assertThat(kv.contains(KEY, DOUBLE_2)).isFalse();

        kv.delete(KEY);
        assertThat(kv.contains(KEY)).isFalse();
    }

    @Test
    public void testContainsFloat() {
        kv.set(KEY, FLOAT_1);

        assertThat(kv.contains(KEY)).isTrue();
        assertThat(kv.contains(KEY, FLOAT_1)).isTrue();
        assertThat(kv.contains(KEY, FLOAT_2)).isFalse();

        kv.delete(KEY);
        assertThat(kv.contains(KEY)).isFalse();
    }

    @Test
    public void testContainsInt() {
        kv.set(KEY, INT_1);

        assertThat(kv.contains(KEY)).isTrue();
        assertThat(kv.contains(KEY, INT_1)).isTrue();
        assertThat(kv.contains(KEY, INT_2)).isFalse();

        kv.delete(KEY);
        assertThat(kv.contains(KEY)).isFalse();
    }

    @Test
    public void testContainsLong() {
        kv.set(KEY, LONG_1);

        assertThat(kv.contains(KEY)).isTrue();
        assertThat(kv.contains(KEY, LONG_1)).isTrue();
        assertThat(kv.contains(KEY, LONG_2)).isFalse();

        kv.delete(KEY);
        assertThat(kv.contains(KEY)).isFalse();
    }

    @Test
    public void testContainsShort() {
        kv.set(KEY, SHORT_1);

        assertThat(kv.contains(KEY)).isTrue();
        assertThat(kv.contains(KEY, SHORT_1)).isTrue();
        assertThat(kv.contains(KEY, SHORT_2)).isFalse();

        kv.delete(KEY);
        assertThat(kv.contains(KEY)).isFalse();
    }

    @Test
    public void testContainsString() {
        kv.set(KEY, STRING_1);

        assertThat(kv.contains(KEY)).isTrue();
        assertThat(kv.contains(KEY, STRING_1)).isTrue();
        assertThat(kv.contains(KEY, STRING_2)).isFalse();

        kv.delete(KEY);
        assertThat(kv.contains(KEY)).isFalse();
    }

    @Test
    public void testDelete() {
        String key = "key1";
        String val1 = "val1";

        String val = kv.get(key);
        assertThat(val).isNull();

        kv.set(key, val1);

        val = kv.get(key);
        assertThat(val).isEqualTo(val1);

        boolean success = kv.delete(key);
        assertTrue(success);

        val = kv.get(key);
        assertThat(val).isNull();

        success = kv.delete(key);
        assertFalse(success);

        val = kv.get(key);
        assertThat(val).isNull();
    }

    @Test
    public void testDeleteNonExistingKey() {
        boolean success = kv.delete(KEY);
        assertThat(success).isFalse();
    }

    @Test
    public void testDeleteBoolean() {
        kv.set(KEY, true);

        boolean success = kv.delete(KEY);
        assertThat(success).isTrue();
    }

    @Test
    public void testDeleteByte() {
        kv.set(KEY, BYTE_1);

        boolean success = kv.delete(KEY);
        assertThat(success).isTrue();
    }

    @Test
    public void testDeleteByteArray() {
        kv.set(KEY, BYTES_1);

        boolean success = kv.delete(KEY);
        assertThat(success).isTrue();
    }

    @Test
    public void testDeleteChar() {
        kv.set(KEY, CHAR_1);

        boolean success = kv.delete(KEY);
        assertThat(success).isTrue();
    }

    @Test
    public void testDeleteDouble() {
        kv.set(KEY, DOUBLE_1);

        boolean success = kv.delete(KEY);
        assertThat(success).isTrue();
    }

    @Test
    public void testDeleteFloat() {
        kv.set(KEY, FLOAT_1);

        boolean success = kv.delete(KEY);
        assertThat(success).isTrue();
    }

    @Test
    public void testDeleteInt() {
        kv.set(KEY, INT_1);

        boolean success = kv.delete(KEY);
        assertThat(success).isTrue();
    }

    @Test
    public void testDeleteLong() {
        kv.set(KEY, LONG_1);

        boolean success = kv.delete(KEY);
        assertThat(success).isTrue();
    }

    @Test
    public void testDeleteShort() {
        kv.set(KEY, SHORT_1);

        boolean success = kv.delete(KEY);
        assertThat(success).isTrue();
    }

    @Test
    public void testDeleteString() {
        kv.set(KEY, STRING_1);

        boolean success = kv.delete(KEY);
        assertThat(success).isTrue();
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveNullKey() {
        kv.remove(null);
    }

    @Test
    public void testRemoveNonExistingKey() {
        Object o = kv.remove(KEY);
        assertThat(o).isNull();
    }

    @Test
    public void testRemoveBoolean() {
        kv.set(KEY, true);

        assertThat(kv.remove(KEY, false)).isFalse();
        assertThat(kv.remove(KEY, true)).isTrue();

        kv.set(KEY, false);

        boolean b = kv.remove(KEY);
        assertThat(b).isFalse();
    }

    @Test
    public void testRemoveByte() {
        kv.set(KEY, BYTE_1);

        assertThat(kv.remove(KEY, BYTE_2)).isFalse();
        assertThat(kv.remove(KEY, BYTE_1)).isTrue();

        kv.set(KEY, BYTE_2);

        byte b = kv.remove(KEY);
        assertThat(b).isEqualTo(BYTE_2);
    }

    @Test
    public void testRemoveByteArray() {
        kv.set(KEY, BYTES_1);

        assertThat(kv.remove(KEY, BYTES_2)).isFalse();
        assertThat(kv.remove(KEY, BYTES_1)).isTrue();

        kv.set(KEY, BYTES_2);

        byte[] b = kv.remove(KEY);
        assertThat(b).isEqualTo(BYTES_2);
    }

    @Test
    public void testRemoveChar() {
        kv.set(KEY, CHAR_1);

        assertThat(kv.remove(KEY, CHAR_2)).isFalse();
        assertThat(kv.remove(KEY, CHAR_1)).isTrue();

        kv.set(KEY, CHAR_2);

        char c = kv.remove(KEY);
        assertThat(c).isEqualTo(CHAR_2);
    }

    @Test
    public void testRemoveDouble() {
        kv.set(KEY, DOUBLE_1);

        assertThat(kv.remove(KEY, DOUBLE_2)).isFalse();
        assertThat(kv.remove(KEY, DOUBLE_1)).isTrue();

        kv.set(KEY, DOUBLE_2);

        double d = kv.remove(KEY);
        assertThat(d).isEqualTo(DOUBLE_2);
    }

    @Test
    public void testRemoveFloat() {
        kv.set(KEY, FLOAT_1);

        assertThat(kv.remove(KEY, FLOAT_2)).isFalse();
        assertThat(kv.remove(KEY, FLOAT_1)).isTrue();

        kv.set(KEY, FLOAT_2);

        float f = kv.remove(KEY);
        assertThat(f).isEqualTo(FLOAT_2);
    }

    @Test
    public void testRemoveInt() {
        kv.set(KEY, INT_1);

        assertThat(kv.remove(KEY, INT_2)).isFalse();
        assertThat(kv.remove(KEY, INT_1)).isTrue();

        kv.set(KEY, INT_2);

        int i = kv.remove(KEY);
        assertThat(i).isEqualTo(INT_2);
    }

    @Test
    public void testRemoveLong() {
        kv.set(KEY, LONG_1);

        assertThat(kv.remove(KEY, LONG_2)).isFalse();
        assertThat(kv.remove(KEY, LONG_1)).isTrue();

        kv.set(KEY, LONG_2);

        long l = kv.remove(KEY);
        assertThat(l).isEqualTo(LONG_2);
    }

    @Test
    public void testRemoveShort() {
        kv.set(KEY, SHORT_1);

        assertThat(kv.remove(KEY, SHORT_2)).isFalse();
        assertThat(kv.remove(KEY, SHORT_1)).isTrue();

        kv.set(KEY, SHORT_2);

        short s = kv.remove(KEY);
        assertThat(s).isEqualTo(SHORT_2);
    }

    @Test
    public void testRemoveString() {
        kv.set(KEY, STRING_1);

        assertThat(kv.remove(KEY, STRING_2)).isFalse();
        assertThat(kv.remove(KEY, STRING_1)).isTrue();

        kv.set(KEY, STRING_2);

        String s = kv.remove(KEY);
        assertThat(s).isEqualTo(STRING_2);
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceNullKey() {
        kv.replace(null, STRING_1, STRING_2);
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceNullOldValue() {
        kv.replace(KEY, null, STRING_1);
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceNullNewValue() {
        kv.replace(KEY, STRING_1, null);
    }

    @Test
    public void testReplaceBoolean() {
        kv.set(KEY, false);

        assertThat(kv.replace(KEY, true, false)).isFalse();
        assertThat(kv.replace(KEY, true, STRING_1)).isFalse();
        assertThat(kv.replace(KEY, false, true)).isTrue();
        assertThat(kv.replace(KEY, true, STRING_1)).isTrue();
    }

    @Test
    public void testReplaceByte() {
        kv.set(KEY, BYTE_1);

        assertThat(kv.replace(KEY, BYTE_2, BYTE_1)).isFalse();
        assertThat(kv.replace(KEY, BYTE_2, STRING_1)).isFalse();
        assertThat(kv.replace(KEY, BYTE_1, BYTE_2)).isTrue();
        assertThat(kv.replace(KEY, BYTE_2, STRING_1)).isTrue();
    }

    @Test
    public void testReplaceByteArray() {
        kv.set(KEY, BYTES_1);

        assertThat(kv.replace(KEY, BYTES_2, BYTES_1)).isFalse();
        assertThat(kv.replace(KEY, BYTES_2, STRING_1)).isFalse();
        assertThat(kv.replace(KEY, BYTES_1, BYTES_2)).isTrue();
        assertThat(kv.replace(KEY, BYTES_2, STRING_1)).isTrue();
    }

    @Test
    public void testReplaceChar() {
        kv.set(KEY, CHAR_1);

        assertThat(kv.replace(KEY, CHAR_2, CHAR_1)).isFalse();
        assertThat(kv.replace(KEY, CHAR_2, STRING_1)).isFalse();
        assertThat(kv.replace(KEY, CHAR_1, CHAR_2)).isTrue();
        assertThat(kv.replace(KEY, CHAR_2, STRING_1)).isTrue();
    }

    @Test
    public void testReplaceDouble() {
        kv.set(KEY, DOUBLE_1);

        assertThat(kv.replace(KEY, DOUBLE_2, DOUBLE_1)).isFalse();
        assertThat(kv.replace(KEY, DOUBLE_2, STRING_1)).isFalse();
        assertThat(kv.replace(KEY, DOUBLE_1, DOUBLE_2)).isTrue();
        assertThat(kv.replace(KEY, DOUBLE_2, STRING_1)).isTrue();
    }

    @Test
    public void testReplaceFloat() {
        kv.set(KEY, FLOAT_1);

        assertThat(kv.replace(KEY, FLOAT_2, FLOAT_1)).isFalse();
        assertThat(kv.replace(KEY, FLOAT_2, STRING_1)).isFalse();
        assertThat(kv.replace(KEY, FLOAT_1, FLOAT_2)).isTrue();
        assertThat(kv.replace(KEY, FLOAT_2, STRING_1)).isTrue();
    }

    @Test
    public void testReplaceInt() {
        kv.set(KEY, INT_1);

        assertThat(kv.replace(KEY, INT_2, INT_1)).isFalse();
        assertThat(kv.replace(KEY, INT_2, STRING_1)).isFalse();
        assertThat(kv.replace(KEY, INT_1, INT_2)).isTrue();
        assertThat(kv.replace(KEY, INT_2, STRING_1)).isTrue();
    }

    @Test
    public void testReplaceLong() {
        kv.set(KEY, LONG_1);

        assertThat(kv.replace(KEY, LONG_2, LONG_1)).isFalse();
        assertThat(kv.replace(KEY, LONG_2, STRING_1)).isFalse();
        assertThat(kv.replace(KEY, LONG_1, LONG_2)).isTrue();
        assertThat(kv.replace(KEY, LONG_2, STRING_1)).isTrue();
    }

    @Test
    public void testReplaceShort() {
        kv.set(KEY, SHORT_1);

        assertThat(kv.replace(KEY, SHORT_2, SHORT_1)).isFalse();
        assertThat(kv.replace(KEY, SHORT_2, STRING_1)).isFalse();
        assertThat(kv.replace(KEY, SHORT_1, SHORT_2)).isTrue();
        assertThat(kv.replace(KEY, SHORT_2, STRING_1)).isTrue();
    }

    @Test
    public void testReplaceString() {
        kv.set(KEY, STRING_1);

        assertThat(kv.replace(KEY, STRING_2, STRING_1)).isFalse();
        assertThat(kv.replace(KEY, STRING_2, INT_1)).isFalse();
        assertThat(kv.replace(KEY, STRING_1, STRING_2)).isTrue();
        assertThat(kv.replace(KEY, STRING_2, INT_1)).isTrue();
    }

    @Test
    public void testSize() {
        int keyCount = 100;
        for (int i = 0; i < keyCount; i++) {
            String key = "key" + i;
            String val = "val" + i;

            kv.set(key, val);

            int size = kv.size();
            assertThat(size).isEqualTo(i + 1);
        }

        for (int i = 0; i < keyCount; i++) {
            String key = "key" + i;

            kv.delete(key);

            int size = kv.size();
            assertThat(size).isEqualTo(100 - i - 1);
        }
    }

    @Test
    public void testClear() {
        int keyCount = 100;
        for (int i = 0; i < keyCount; i++) {
            String key = "key" + i;
            String val = "val" + i;

            kv.set(key, val);
        }

        assertThat(kv.isEmpty()).isFalse();

        int deleted = kv.clear();
        assertThat(deleted).isEqualTo(keyCount);

        assertThat(kv.isEmpty()).isTrue();
    }

}
