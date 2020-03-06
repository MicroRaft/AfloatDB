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

package io.afloatdb;

import com.google.protobuf.ByteString;
import com.typesafe.config.ConfigFactory;
import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.internal.serialization.Serialization;
import io.afloatdb.kv.proto.PutRequest;
import io.afloatdb.kv.proto.TypedValue;
import io.afloatdb.raft.proto.ProtoOperation;
import io.microraft.RaftNode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.afloatdb.utils.AfloatDBTestUtils.getRaftNode;
import static io.afloatdb.utils.AfloatDBTestUtils.waitUntilLeaderElected;

@Fork(1)
public class AfloatDBBenchmark {

    public static void main(String[] args)
            throws RunnerException {
        Options opt = new OptionsBuilder().include(AfloatDBBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Threads(5)
    public void setTesting(Context context) {
        String key = "key" + context.random.nextInt(10000);
        TypedValue typedValue = TypedValue.newBuilder().setType(Serialization.STRING_TYPE).setValue(ByteString.copyFromUtf8(key))
                                          .build();
        PutRequest request = PutRequest.newBuilder().setKey(key).setValue(typedValue).build();
        context.leader.replicate(ProtoOperation.newBuilder().setPutRequest(request).build()).join();
    }

    @State(Scope.Benchmark)
    public static class Context {

        Random random = new Random();
        private AfloatDBConfig config1 = AfloatDBConfig.from(ConfigFactory.load("node1.conf"));
        private AfloatDBConfig config2 = AfloatDBConfig.from(ConfigFactory.load("node2.conf"));
        private AfloatDBConfig config3 = AfloatDBConfig.from(ConfigFactory.load("node3.conf"));

        private List<AfloatDB> servers = new ArrayList<>();
        private RaftNode leader;

        @Setup(Level.Trial)
        public void setup() {
            servers.add(AfloatDB.bootstrap(config1));
            servers.add(AfloatDB.bootstrap(config2));
            servers.add(AfloatDB.bootstrap(config3));
            leader = getRaftNode(waitUntilLeaderElected(servers));
        }

        @TearDown(Level.Trial)
        public void tearDown(Blackhole hole) {
            servers.forEach(AfloatDB::shutdown);
        }

    }

}
