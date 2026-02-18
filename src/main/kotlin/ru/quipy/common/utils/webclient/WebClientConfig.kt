package ru.quipy.common.utils.webclient

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.client.WebClient
import io.netty.channel.ChannelOption
import reactor.netty.http.HttpProtocol
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import java.time.Duration

@Configuration
class WebClientConfig {

    @Bean
    fun webClient(): WebClient {
        val connectionProvider = ConnectionProvider
            .builder("payment-connection-pool")
            .maxConnections(10000)
            .maxIdleTime(Duration.ofSeconds(30))
            .maxLifeTime(Duration.ofSeconds(120))
            .pendingAcquireTimeout(Duration.ofSeconds(60))
            .evictInBackground(Duration.ofSeconds(120))
            .build()

        val httpClient = HttpClient
            .create(connectionProvider)
            .protocol(HttpProtocol.HTTP11)
            .responseTimeout(Duration.ofSeconds(10))
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            .keepAlive(true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.TCP_NODELAY, true)

        return WebClient
            .builder()
            .clientConnector(ReactorClientHttpConnector(httpClient))
            .build()
    }
}
