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

import io.afloatdb.client.config.AfloatDBClientConfig;
import io.afloatdb.internal.lifecycle.ProcessTerminationReporter;
import io.afloatdb.internal.lifecycle.TerminationAware;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.afloatdb.client.internal.di.AfloatDBClientModule.CLIENT_ID_KEY;
import static io.afloatdb.client.internal.di.AfloatDBClientModule.CONFIG_KEY;

@Singleton
public class ManagedChannelSupplier
        implements Supplier<ManagedChannel>, TerminationAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedChannelSupplier.class);

    private final String clientId;
    private final ManagedChannel channel;
    private final ProcessTerminationReporter processTerminationReporter;

    @Inject
    public ManagedChannelSupplier(@Named(CLIENT_ID_KEY) String clientId, @Named(CONFIG_KEY) AfloatDBClientConfig config,
                                  ProcessTerminationReporter processTerminationReporter) {
        this.clientId = clientId;
        this.channel = ManagedChannelBuilder.forTarget(config.getServerAddress()).directExecutor().usePlaintext().build();
        this.processTerminationReporter = processTerminationReporter;
    }

    @Override
    public ManagedChannel get() {
        return channel;
    }

    @PreDestroy
    public void shutdown() {
        channel.shutdownNow();
        if (processTerminationReporter.isCurrentProcessTerminating()) {
            System.err.println(clientId + " server channel is shut down.");
        } else {
            LOGGER.warn("{} managed channel is shut down.", clientId);
        }
    }

    @Override
    public void awaitTermination() {
        try {
            if (!channel.awaitTermination(10, TimeUnit.SECONDS)) {
                if (processTerminationReporter.isCurrentProcessTerminating()) {
                    System.err.println(clientId + " await termination of the server channel timed-out!");
                } else {
                    LOGGER.warn("{} await termination of the server channel timed-out!", clientId);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            if (processTerminationReporter.isCurrentProcessTerminating()) {
                System.err.println(clientId + " await termination of the server channel interrupted!");
            } else {
                LOGGER.warn("{} await termination of the server channel interrupted!", clientId);
            }
        }
    }

}
