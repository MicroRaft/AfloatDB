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

package io.afloatdb.client.internal.rpc.impl;

import io.afloatdb.kv.proto.KVServiceGrpc;
import io.afloatdb.kv.proto.KVServiceGrpc.KVServiceBlockingStub;
import io.grpc.ManagedChannel;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.function.Supplier;

import static io.afloatdb.client.internal.di.AfloatDBClientModule.MANAGED_CHANNEL_KEY;

@Singleton
public class KVServiceBlockingStubSupplier
        implements Supplier<KVServiceBlockingStub> {

    private final KVServiceBlockingStub stub;

    @Inject
    public KVServiceBlockingStubSupplier(@Named(MANAGED_CHANNEL_KEY) Supplier<ManagedChannel> supplier) {
        this.stub = KVServiceGrpc.newBlockingStub(supplier.get());
    }

    @Override
    public KVServiceBlockingStub get() {
        return stub;
    }

}
