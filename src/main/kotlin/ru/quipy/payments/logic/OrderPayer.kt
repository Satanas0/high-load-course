package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.UUID
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(registry: MeterRegistry) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        private const val THREAD_COUNT = 1000
        private const val DEADLINE_GUARD_MS = 500L
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @OptIn(DelicateCoroutinesApi::class)
    private val executorScope = CoroutineScope(
        SupervisorJob() + newFixedThreadPoolContext(THREAD_COUNT, "io_pool")
    )

    private val paymentExecutionTimer = registry.timer("payment_executor_task_duration")

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        executorScope.launch {
            val queuedAt = System.currentTimeMillis()
            if (deadline - queuedAt < DEADLINE_GUARD_MS) {
                logger.warn("Payment $paymentId deadline expired while queued (${deadline - queuedAt}ms remaining), skipping")
                return@launch
            }
            val start = System.nanoTime()
            try {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                paymentExecutionTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS)
            } catch (e: Exception) {
                logger.error("Failed to process payment $paymentId for order $orderId", e)
            }
        }

        return createdAt
    }
}
