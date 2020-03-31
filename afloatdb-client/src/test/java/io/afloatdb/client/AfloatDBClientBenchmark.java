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

import io.afloatdb.client.config.AfloatDBClientConfig;
import io.afloatdb.client.config.AfloatDBClientConfig.AfloatDBClientConfigBuilder;
import io.afloatdb.client.kvstore.KV;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(1)
public class AfloatDBClientBenchmark {

    public static void main(String[] args)
            throws RunnerException {
        Options opt = new OptionsBuilder().include(AfloatDBClientBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Threads(5)
    @Measurement(iterations = 100)
    public void setTesting(Context context) {
        int i = context.random.nextInt(50000);
        String keyStr = "key" + i;
        String valueStr = "value" + i;
        context.kv.set(keyStr, valueStr);
    }

    @State(Scope.Benchmark)
    public static class Context {
        AfloatDBClient client;
        Random random = new Random();
        KV kv;

        @Setup(Level.Trial)
        public void setup() {
            String target = "localhost:6701";
            AfloatDBClientConfigBuilder configBuilder = AfloatDBClientConfig.newBuilder();
            AfloatDBClientConfig clientConfig = configBuilder.setClientId("benchmark-client").setServerAddress(target).build();

            client = AfloatDBClient.newInstance(clientConfig);
            kv = client.getKV();
            kv.clear();
        }

        @TearDown(Level.Trial)
        public void tearDown(Blackhole hole) {
            client.shutdown();
        }

    }

}
