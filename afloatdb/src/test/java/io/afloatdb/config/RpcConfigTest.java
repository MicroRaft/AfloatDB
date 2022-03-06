package io.afloatdb.config;

import io.microraft.test.util.BaseTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RpcConfigTest extends BaseTest {

    @Test
    public void when_rpcTimeoutSecsProvided_then_shouldCreateConfigWithProvidedValue() {
        int rpcTimeoutSecs = 45;
        RpcConfig config = RpcConfig.newBuilder().setRpcTimeoutSecs(rpcTimeoutSecs).build();

        assertThat(config.getRpcTimeoutSecs()).isEqualTo(rpcTimeoutSecs);
    }

    @Test
    public void when_rpcTimeoutSecsNotProvided_then_shouldCreateConfigWithDefaultValue() {
        RpcConfig config = RpcConfig.newBuilder().build();

        assertThat(config.getRpcTimeoutSecs()).isEqualTo(RpcConfig.DEFAULT_RPC_TIMEOUT_SECONDS);
    }

    @Test
    public void when_retryLimitProvided_then_shouldCreateConfigWithProvidedValue() {
        int retryLimit = 25;
        RpcConfig config = RpcConfig.newBuilder().setRetryLimit(retryLimit).build();

        assertThat(config.getRetryLimit()).isEqualTo(retryLimit);
    }

    @Test
    public void when_retryLimitNotProvided_then_shouldCreateConfigWithDefaultValue() {
        RpcConfig config = RpcConfig.newBuilder().build();

        assertThat(config.getRetryLimit()).isEqualTo(RpcConfig.DEFAULT_RETRY_LIMIT);
    }

}
