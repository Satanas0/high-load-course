package ru.quipy.config

import jakarta.annotation.PostConstruct
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory
import org.eclipse.jetty.server.ServerConnector
import org.slf4j.LoggerFactory
import org.springframework.boot.web.embedded.jetty.JettyServerCustomizer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory
import org.springframework.boot.web.server.WebServerFactoryCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.streams.AggregateEventStreamManager
import java.util.*


/**
 * This files contains some configurations that you might want to have in your project. Some configurations are
 * made in for the sake of demonstration and not required for the library functioning. Usually you can have even
 * more minimalistic config
 *
 * Take into consideration that we autoscan files searching for Aggregates, Events and StateTransition functions.
 * Autoscan enabled via [event.sourcing.auto-scan-enabled] property.
 *
 * But you can always disable it and register all the classes manually like this
 * ```
 * @Autowired
 * private lateinit var aggregateRegistry: AggregateRegistry
 *
 * aggregateRegistry.register(ProjectAggregate::class, ProjectAggregateState::class) {
 *     registerStateTransition(TagCreatedEvent::class, ProjectAggregateState::tagCreatedApply)
 *     registerStateTransition(TaskCreatedEvent::class, ProjectAggregateState::taskCreatedApply)
 *     registerStateTransition(TagAssignedToTaskEvent::class, ProjectAggregateState::tagAssignedApply)
 * }
 * ```
 */
@Configuration
class EventSourcingLibConfiguration {

    private val logger = LoggerFactory.getLogger(EventSourcingLibConfiguration::class.java)

    @Autowired
    private lateinit var eventSourcingServiceFactory: EventSourcingServiceFactory

    @Autowired
    private lateinit var eventStreamManager: AggregateEventStreamManager

    /**
     * Use this object to create/update the aggregate
     */
    @Bean
    fun paymentsEsService() = eventSourcingServiceFactory.create<UUID, PaymentAggregate, PaymentAggregateState>()

    @Bean
    fun jettyHttp2Customizer(
        @Value("\${server.jetty.http2.max-concurrent-streams:10000}") maxStreams: Int
    ): WebServerFactoryCustomizer<JettyServletWebServerFactory> {
        return WebServerFactoryCustomizer { factory ->
            factory.addServerCustomizers(JettyServerCustomizer { server ->
                for (connector in server.connectors) {
                    if (connector is ServerConnector) {
                        for (cf in connector.connectionFactories) {
                            if (cf is HTTP2CServerConnectionFactory) {
                                cf.maxConcurrentStreams = maxStreams
                            }
                        }
                    }
                }
            })
        }
    }

    @PostConstruct
    fun init() {
        // Demonstrates how you can set up the listeners to the event stream
        eventStreamManager.maintenance {
            onRecordHandledSuccessfully { streamName, eventName ->
                logger.debug("Stream $streamName successfully processed record of $eventName")
            }

            onBatchRead { streamName, batchSize ->
                logger.debug("Stream $streamName read batch size: $batchSize")
            }
        }
    }

}
