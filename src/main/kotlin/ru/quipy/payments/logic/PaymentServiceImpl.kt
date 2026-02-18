package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.atomic.AtomicInteger


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    private val enabledAdapters = paymentAccounts.filter { it.isEnabled() }
    private val adapterIndex = AtomicInteger(0)

    override suspend fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        if (enabledAdapters.isEmpty()) {
            logger.warn("No enabled payment adapters available")
            return
        }
        val size = enabledAdapters.size
        val startIdx = Math.floorMod(adapterIndex.getAndIncrement(), size)
        val adapter = (0 until size)
            .map { enabledAdapters[(startIdx + it) % size] }
            .firstOrNull { it.hasCapacity() }
            ?: enabledAdapters[startIdx]
        adapter.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
    }
}
