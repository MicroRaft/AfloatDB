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

package io.afloatdb.client.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.afloatdb.client.AfloatDBClientException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class AfloatDBClientConfig {

    private Config config;
    private String clientId;
    private String serverAddress;

    private AfloatDBClientConfig() {
    }

    public static AfloatDBClientConfig from(Config config) {
        return newBuilder().setConfig(config).build();
    }

    public static AfloatDBClientConfigBuilder newBuilder() {
        return new AfloatDBClientConfigBuilder();
    }

    @Nonnull
    public Config getConfig() {
        return config;
    }

    @Nullable
    public String getClientId() {
        return clientId;
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public static class AfloatDBClientConfigBuilder {

        private AfloatDBClientConfig clientConfig = new AfloatDBClientConfig();

        public AfloatDBClientConfigBuilder setConfig(Config config) {
            requireNonNull(config);
            clientConfig.config = config;
            return this;
        }

        public AfloatDBClientConfigBuilder setClientId(String clientId) {
            requireNonNull(clientId);
            clientConfig.clientId = clientId;
            return this;
        }

        public AfloatDBClientConfigBuilder setServerAddress(String serverAddress) {
            requireNonNull(serverAddress);
            clientConfig.serverAddress = serverAddress;
            return this;
        }

        public AfloatDBClientConfig build() {
            if (clientConfig == null) {
                throw new AfloatDBClientException("AfloatDBClientConfig is already built!");
            }

            if (clientConfig.config == null) {
                try {
                    clientConfig.config = ConfigFactory.load();
                } catch (Exception e) {
                    throw new AfloatDBClientException("Could not load Config!", e);
                }
            }

            try {
                if (clientConfig.config.hasPath("afloatdb.client")) {
                    Config config = clientConfig.config.getConfig("afloatdb.client");

                    if (clientConfig.clientId == null && config.hasPath("id")) {
                        clientConfig.clientId = config.getString("id");
                    }

                    if (clientConfig.serverAddress == null && config.hasPath("server-address")) {
                        clientConfig.serverAddress = config.getString("server-address");
                    }
                }
            } catch (Exception e) {
                if (e instanceof AfloatDBClientException) {
                    throw (AfloatDBClientException) e;
                }

                throw new AfloatDBClientException("Could not build AfloatDBClientConfig!", e);
            }

            if (clientConfig.clientId == null) {
                clientConfig.clientId = "Client<" + UUID.randomUUID().toString() + ">";
            }

            if (clientConfig.serverAddress == null) {
                throw new AfloatDBClientException("Server address is missing!");
            }

            AfloatDBClientConfig clientConfig = this.clientConfig;
            this.clientConfig = null;
            return clientConfig;
        }

    }

}
