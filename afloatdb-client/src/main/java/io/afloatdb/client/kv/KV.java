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

package io.afloatdb.client.kv;

import javax.annotation.Nonnull;

public interface KV
        extends Put, Set, Get, Contains, Delete, Remove, Replace {

    default boolean isEmpty() {
        return isEmpty(0L);
    }

    default boolean isEmpty(long minCommitIndex) {
        return size(minCommitIndex) == 0;
    }

    default @Nonnull
    Ordered<Boolean> isEmptyOrdered() {
        return isEmptyOrdered(0L);
    }

    @Nonnull
    Ordered<Boolean> isEmptyOrdered(long minCommitIndex);

    default int size() {
        return size(0L);
    }

    int size(long minCommitIndex);

    default @Nonnull
    Ordered<Integer> sizeOrdered() {
        return sizeOrdered(0L);
    }

    @Nonnull
    Ordered<Integer> sizeOrdered(long minCommitIndex);

    int clear();

    @Nonnull
    Ordered<Integer> clearOrdered();

}
