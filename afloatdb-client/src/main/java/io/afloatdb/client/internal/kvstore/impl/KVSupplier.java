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
import io.afloatdb.kv.proto.KVServiceGrpc.KVServiceBlockingStub;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.function.Supplier;

import static io.afloatdb.client.internal.di.AfloatDBClientModule.BLOCKING_STUB_KEY;

@Singleton
public class KVSupplier
        implements Supplier<KV> {

    private final KV kv;

    @Inject
    public KVSupplier(@Named(BLOCKING_STUB_KEY) Supplier<KVServiceBlockingStub> stubSupplier) {
        this.kv = new KVProxy(stubSupplier.get());
    }

    @Override
    public KV get() {
        return kv;
    }

}
