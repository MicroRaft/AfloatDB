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

package io.afloatdb.internal.raft.impl.model;

import io.afloatdb.raft.proto.ProtoRaftEndpoint;
import io.microraft.RaftEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AfloatDBEndpoint
        implements RaftEndpoint {

    private static final ConcurrentMap<String, AfloatDBEndpoint> cache = new ConcurrentHashMap<>();
    private ProtoRaftEndpoint endpoint;

    public AfloatDBEndpoint(ProtoRaftEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public static AfloatDBEndpoint wrap(@Nonnull ProtoRaftEndpoint endpoint) {
        return cache.computeIfAbsent(endpoint.getId(), id -> new AfloatDBEndpoint(endpoint));
    }

    public static ProtoRaftEndpoint extract(@Nullable RaftEndpoint endpoint) {
        return endpoint != null ? ((AfloatDBEndpoint) endpoint).getEndpoint() : null;
    }

    public ProtoRaftEndpoint getEndpoint() {
        return endpoint;
    }

    @Override
    public int hashCode() {
        return endpoint.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AfloatDBEndpoint that = (AfloatDBEndpoint) o;

        return endpoint.equals(that.endpoint);
    }

    @Override
    public String toString() {
        return "AfloatDBEndpoint{" + "id=" + getId() + '}';
    }

    @Nonnull
    @Override
    public Object getId() {
        return endpoint.getId();
    }

}
