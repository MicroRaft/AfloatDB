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

package io.afloatdb;

import io.afloatdb.config.AfloatDBConfig;
import io.afloatdb.internal.AfloatDBImpl.AfloatDBBootstrapper;
import io.afloatdb.internal.AfloatDBImpl.AfloatDBJoiner;
import io.microraft.RaftEndpoint;
import io.microraft.report.RaftNodeReport;

import javax.annotation.Nonnull;

public interface AfloatDB {

    static AfloatDB bootstrap(AfloatDBConfig config) {
        return new AfloatDBBootstrapper(config).get();
    }

    static AfloatDB join(AfloatDBConfig config) {
        return new AfloatDBJoiner(config).get();
    }

    @Nonnull
    AfloatDBConfig getConfig();

    @Nonnull
    RaftEndpoint getLocalEndpoint();

    @Nonnull
    RaftNodeReport getRaftNodeReport();

    void shutdown();

    boolean isShutdown();

    void awaitTermination();

}
