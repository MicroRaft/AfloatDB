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

package io.afloatdb.internal.raft.impl;

import io.afloatdb.internal.rpc.RaftMessageDispatcher;
import io.microraft.RaftEndpoint;
import io.microraft.integration.RaftNodeRuntime;
import io.microraft.model.message.RaftMessage;
import io.microraft.report.RaftNodeReport;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.afloatdb.internal.di.AfloatDBModule.RAFT_NODE_EXECUTOR_KEY;

@Singleton
public class AfloatDBRaftNodeRuntime
        implements RaftNodeRuntime, Supplier<RaftNodeReport> {

    private final ScheduledExecutorService executor;
    private final RaftMessageDispatcher raftMessageDispatcher;
    private volatile RaftNodeReport lastStateSnapshot;

    @Inject
    public AfloatDBRaftNodeRuntime(@Named(RAFT_NODE_EXECUTOR_KEY) ScheduledExecutorService executor,
                                   RaftMessageDispatcher raftMessageDispatcher) {
        this.executor = executor;
        this.raftMessageDispatcher = raftMessageDispatcher;
    }

    @Override
    public void execute(@Nonnull Runnable task) {
        if (Thread.currentThread() instanceof RaftNodeThread) {
            task.run();
        } else {
            executor.submit(task);
        }
    }

    @Override
    public void submit(@Nonnull Runnable task) {
        executor.submit(task);
    }

    @Override
    public void schedule(@Nonnull Runnable task, long delay, @Nonnull TimeUnit timeUnit) {
        executor.schedule(task, delay, timeUnit);
    }

    @Override
    public void send(@Nonnull RaftEndpoint target, @Nonnull RaftMessage message) {
        raftMessageDispatcher.send(target, message);
    }

    @Override
    public void onRaftNodeReport(@Nonnull RaftNodeReport snapshot) {
        this.lastStateSnapshot = snapshot;
    }

    @Override
    public void onRaftGroupTerminated() {
    }

    @Override
    public RaftNodeReport get() {
        return lastStateSnapshot;
    }

}
