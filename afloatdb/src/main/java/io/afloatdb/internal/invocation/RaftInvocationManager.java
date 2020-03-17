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

package io.afloatdb.internal.invocation;

import io.afloatdb.raft.proto.ProtoOperation;
import io.microraft.Ordered;
import io.microraft.QueryPolicy;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

public interface RaftInvocationManager {

    <T> CompletableFuture<Ordered<T>> invoke(@Nonnull ProtoOperation operation);

    <T> CompletableFuture<Ordered<T>> query(@Nonnull ProtoOperation operation, @Nonnull QueryPolicy queryPolicy,
                                            long minCommitRequest);
}
