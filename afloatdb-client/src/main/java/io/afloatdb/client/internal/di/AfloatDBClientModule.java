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

package io.afloatdb.client.internal.di;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import io.afloatdb.client.config.AfloatDBClientConfig;
import io.afloatdb.client.internal.kvstore.impl.KVSupplier;
import io.afloatdb.client.internal.rpc.impl.KVServiceBlockingStubSupplier;
import io.afloatdb.client.internal.rpc.impl.ManagedChannelSupplier;
import io.afloatdb.client.kvstore.KV;
import io.afloatdb.internal.lifecycle.ProcessTerminationReporter;
import io.afloatdb.internal.lifecycle.impl.ProcessTerminationReporterImpl;
import io.afloatdb.kv.proto.KVServiceGrpc.KVServiceBlockingStub;
import io.grpc.ManagedChannel;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.google.inject.name.Names.named;

public class AfloatDBClientModule
        extends AbstractModule {

    public static final String CLIENT_ID_KEY = "ClientId";
    public static final String CONFIG_KEY = "Config";
    public static final String MANAGED_CHANNEL_KEY = "ManagedChannel";
    public static final String BLOCKING_STUB_KEY = "BlockingStub";
    public static final String KV_STORE_KEY = "KVStore";
    public static final String PROCESS_TERMINATION_FLAG_KEY = "ProcessTerminationFlag";

    private final AfloatDBClientConfig config;
    private final AtomicBoolean processTerminationFlag;

    public AfloatDBClientModule(AfloatDBClientConfig config, AtomicBoolean processTerminationFlag) {
        this.config = config;
        this.processTerminationFlag = processTerminationFlag;
    }

    @Override
    protected void configure() {
        bind(String.class).annotatedWith(named(CLIENT_ID_KEY)).toInstance(config.getClientId());
        bind(AfloatDBClientConfig.class).annotatedWith(named(CONFIG_KEY)).toInstance(config);
        bind(AtomicBoolean.class).annotatedWith(named(PROCESS_TERMINATION_FLAG_KEY)).toInstance(processTerminationFlag);
        bind(ProcessTerminationReporter.class).to(ProcessTerminationReporterImpl.class);
        bind(new TypeLiteral<Supplier<ManagedChannel>>() {
        }).annotatedWith(named(MANAGED_CHANNEL_KEY)).to(ManagedChannelSupplier.class);
        bind(new TypeLiteral<Supplier<KVServiceBlockingStub>>() {
        }).annotatedWith(named(BLOCKING_STUB_KEY)).to(KVServiceBlockingStubSupplier.class);
        bind(new TypeLiteral<Supplier<KV>>() {
        }).annotatedWith(named(KV_STORE_KEY)).to(KVSupplier.class);
    }

}
