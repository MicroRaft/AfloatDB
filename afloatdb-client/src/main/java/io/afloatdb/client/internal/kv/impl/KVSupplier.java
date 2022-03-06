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

package io.afloatdb.client.internal.kv.impl;

import io.afloatdb.client.kv.KV;
import io.afloatdb.kv.proto.KVRequestHandlerGrpc.KVRequestHandlerBlockingStub;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.function.Supplier;

import static io.afloatdb.client.internal.di.AfloatDBClientModule.KV_STUB_KEY;

@Singleton
public class KVSupplier implements Supplier<KV> {

    private final KV kv;

    @Inject
    public KVSupplier(@Named(KV_STUB_KEY) Supplier<KVRequestHandlerBlockingStub> kvStubSupplier) {
        this.kv = new KVProxy(kvStubSupplier);
    }

    @Override
    public KV get() {
        return kv;
    }

}
