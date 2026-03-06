package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    adapters: List<PaymentExternalSystemAdapter>,
    private val dbScope: CoroutineScope
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val paymentExecutor: ThreadPoolExecutor = ThreadPoolExecutor(
        256,
        256,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        paymentExecutor.submit {
            dbScope.launch {
                while (true) {
                    try {
                        val createdEvent = paymentESService.create {
                            it.create(paymentId, orderId, amount)
                        }
                        logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
                        break
                    } catch (_: IllegalArgumentException) {
                        delay(10)
                    }
                }
            }
            paymentService.submitPaymentRequest(paymentId, amount, orderId, createdAt, deadline)
        }

        return createdAt
    }

    fun getMaxRateLimit() = paymentService.getMaxRateLimit()
}
