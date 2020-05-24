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

package io.afloatdb.client;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.afloatdb.AfloatDB;
import io.afloatdb.client.config.AfloatDBClientConfig;
import io.afloatdb.client.kvstore.KV;
import io.microraft.test.util.BaseTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.typesafe.config.ConfigFactory.load;
import static io.afloatdb.utils.AfloatDBTestUtils.CONFIG_1;
import static io.afloatdb.utils.AfloatDBTestUtils.CONFIG_2;
import static io.afloatdb.utils.AfloatDBTestUtils.CONFIG_3;
import static io.afloatdb.utils.AfloatDBTestUtils.waitUntilLeaderElected;
import static io.microraft.impl.util.RandomPicker.getRandomInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class AfloatDBClientKVTest
        extends BaseTest {

    private static final byte[] BYTES_1 = new byte[]{1, 2, 3, 4};
    private static final byte[] BYTES_2 = new byte[]{4, 3, 2, 1};
    private static final int INT_1 = 753;
    private static final int INT_2 = 1239;
    private static final long LONG_1 = 19238;
    private static final long LONG_2 = 4693;
    private static final String STRING_1 = "str1";
    private static final String STRING_2 = "str2";
    private static final String KEY = "key";

    private AfloatDBClient client;

    private static List<AfloatDB> servers = new ArrayList<>();
    private KV kv;
    private boolean singleConnection;

    public AfloatDBClientKVTest(boolean singleConnection) {
        this.singleConnection = singleConnection;
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @BeforeClass
    public static void initCluster() {
        servers.add(AfloatDB.bootstrap(CONFIG_1));
        servers.add(AfloatDB.bootstrap(CONFIG_2));
        servers.add(AfloatDB.bootstrap(CONFIG_3));
        waitUntilLeaderElected(servers);
    }

    @AfterClass
    public static void shutDownCluster() {
        servers.forEach(AfloatDB::shutdown);
    }

    @Before
    public void initClient() {
        String serverAddress = servers.get(getRandomInt(servers.size())).getConfig().getLocalEndpointConfig().getAddress();
        Config config = ConfigFactory.parseString("afloatdb.client.server-address: \"" + serverAddress + "\"")
                                     .withFallback(load("client.conf"));
        AfloatDBClientConfig clientConfig = AfloatDBClientConfig.newBuilder().setConfig(config)
                                                                .setSingleConnection(singleConnection).build();
        client = AfloatDBClient.newInstance(clientConfig);
        kv = client.getKV();
        kv.clear();
    }

    @After
    public void shutDownClient() {
        if (client != null) {
            client.shutdown();
        }
    }

    @Test(expected = NullPointerException.class)
    public void testPutNullKey() {
        kv.put(null, "val1");
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
    public void testPutByteArrayIfAbsent() {
        byte[] bytes = kv.putIfAbsent(KEY, BYTES_1);
        assertThat(bytes).isNull();

        bytes = kv.get(KEY);
        assertThat(bytes).isEqualTo(BYTES_1);

        bytes = kv.putIfAbsent(KEY, BYTES_2);
        assertThat(bytes).isEqualTo(BYTES_1);
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
    public void testSetByteArray() {
        kv.set(KEY, BYTES_1);

        byte[] b = kv.get(KEY);
        assertThat(b).isEqualTo(BYTES_1);
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
    public void testGetByteArrayOrDefault() {
        byte[] b = kv.getOrDefault(KEY, BYTES_2);
        assertThat(b).isEqualTo(BYTES_2);
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
    public void testContainsByteArray() {
        kv.set(KEY, BYTES_1);

        assertThat(kv.contains(KEY)).isTrue();
        assertThat(kv.contains(KEY, BYTES_1)).isTrue();
        assertThat(kv.contains(KEY, BYTES_2)).isFalse();

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
    public void testDeleteByteArray() {
        kv.set(KEY, BYTES_1);

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
    public void testRemoveByteArray() {
        kv.set(KEY, BYTES_1);

        assertThat(kv.remove(KEY, BYTES_2)).isFalse();
        assertThat(kv.remove(KEY, BYTES_1)).isTrue();

        kv.set(KEY, BYTES_2);

        byte[] b = kv.remove(KEY);
        assertThat(b).isEqualTo(BYTES_2);
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
    public void testReplaceByteArray() {
        kv.set(KEY, BYTES_1);

        assertThat(kv.replace(KEY, BYTES_2, BYTES_1)).isFalse();
        assertThat(kv.replace(KEY, BYTES_2, STRING_1)).isFalse();
        assertThat(kv.replace(KEY, BYTES_1, BYTES_2)).isTrue();
        assertThat(kv.replace(KEY, BYTES_2, STRING_1)).isTrue();
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
