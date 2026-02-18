package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.WebClient
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.metrics.MetricsService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentAggregateState
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID


class PaymentExternalSystemAdapterImpl(
    val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metricsService: MetricsService,
    private val webClient: WebClient
) : PaymentExternalSystemAdapter {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private const val TASK_NAME = "paymentTask"
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(properties.parallelRequests)

    override fun hasCapacity(): Boolean =
        rateLimiter.hasCapacity() && semaphore.availablePermits > 0

    @OptIn(DelicateCoroutinesApi::class)
    private val dbScope = CoroutineScope(SupervisorJob() + newFixedThreadPoolContext(8, "es_write_pool"))

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        val start = now()
        val avgMs = properties.averageProcessingTime.toMillis()
        val remaining = deadline - start
        val maxRetries = 3
        val backoffBase = 10L

        logger.trace("[$accountName] Submitting payment $paymentId (txId=$transactionId), remaining=${remaining}ms")

        val submissionJob: Deferred<Unit> = dbScope.async {
            try {
                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(start - paymentStartedAt))
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Failed to log submission for $paymentId", e)
            }
        }

        if (remaining < avgMs + 100) {
            logger.warn("[$accountName] Skipping payment $paymentId: remaining ${remaining}ms < avg ${avgMs}ms")
            dbScope.launch {
                submissionJob.join()
                try {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline too close")
                    }
                } catch (e: Exception) {
                    logger.error("[$accountName] Failed to log deadline skip for $paymentId", e)
                }
            }
            return
        }

        var attempt = 0
        var success = false
        var lastReason: String? = null

        while (attempt < maxRetries && !success) {
            attempt++

            if (attempt > 1) {
                metricsService.increaseRetryCounter()
            }

            val attemptStart = now()
            val timeLeft = deadline - attemptStart

            if (timeLeft < avgMs + 100) {
                logger.warn("[$accountName] Stop retrying payment $paymentId: deadline too close (${timeLeft}ms left)")
                break
            }
            while (!rateLimiter.tick()) {
                val remaining2 = deadline - now()
                if (remaining2 < avgMs + 100) {
                    logger.warn("[$accountName] Deadline expired while waiting for rate limiter for $paymentId (${remaining2}ms left)")
                    dbScope.launch {
                        submissionJob.join()
                        try {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Deadline expired in rate limiter queue")
                            }
                        } catch (e: Exception) {
                            logger.error("[$accountName] Failed to log deadline expiry for $paymentId", e)
                        }
                    }
                    return
                }
                delay(1)
            }

            try {
                val preCallRemaining = deadline - now()
                if (preCallRemaining <= avgMs) {
                    lastReason = "Deadline passed before HTTP call"
                    break
                }
                val httpTimeoutMs = preCallRemaining.coerceIn(200, 15_000)

                val response = semaphore.withPermit {
                    webClient
                        .post()
                        .uri(
                            "http://$paymentProviderHostPort/external/process" +
                                "?serviceName=$serviceName" +
                                "&token=$token" +
                                "&accountName=$accountName" +
                                "&transactionId=$transactionId" +
                                "&paymentId=$paymentId" +
                                "&amount=$amount"
                        )
                        .accept(MediaType.APPLICATION_JSON)
                        .retrieve()
                        .toEntity(ExternalSysResponse::class.java)
                        .timeout(Duration.ofMillis(httpTimeoutMs))
                        .awaitSingle()
                }

                val body = response.body
                if (body == null) {
                    lastReason = "Empty response body"
                    logger.warn("[$accountName] Empty response for $paymentId (attempt $attempt)")
                    break
                }

                val duration = now() - attemptStart
                metricsService.recordRequestDuration(duration, body.result)

                if (response.statusCode.is2xxSuccessful && body.result) {
                    logger.trace("[$accountName] Payment success for txId=$transactionId, payment=$paymentId")
                    success = true
                    val capturedMessage = body.message
                    dbScope.launch {
                        submissionJob.join()
                        try {
                            paymentESService.update(paymentId) {
                                it.logProcessing(true, now(), transactionId, reason = capturedMessage)
                            }
                        } catch (e: Exception) {
                            logger.error("[$accountName] Failed to log processing success for $paymentId", e)
                        }
                    }
                } else {
                    val retriable = isRetriable(null, response.statusCode.value(), body.message)
                    lastReason = "HTTP ${response.statusCode.value()}: ${body.message}"
                    logger.warn("[$accountName] Payment failed (attempt $attempt/$maxRetries): $lastReason, retriable=$retriable")

                    if (!retriable) break

                    val backoff = (backoffBase * attempt).coerceAtMost(timeLeft / 2)
                    if (backoff > 0) delay(backoff)
                }

                metricsService.incrementCompletedTask(TASK_NAME)
            } catch (e: Exception) {
                val retriable = isRetriable(e, null, e.message)
                lastReason = e.message
                logger.error("[$accountName] Exception during payment $paymentId: ${e.javaClass.simpleName}, retriable=$retriable", e)

                if (!retriable) break

                val backoff = (backoffBase * attempt).coerceAtMost((deadline - now()).coerceAtLeast(0) / 2)
                if (backoff > 0) delay(backoff)
            }
        }

        if (!success) {
            dbScope.launch {
                submissionJob.join()
                try {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = lastReason ?: "Permanent failure")
                    }
                } catch (e: Exception) {
                    logger.error("[$accountName] Failed to log processing failure for $paymentId", e)
                }
            }
        }
    }



    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

private fun isRetriable(e: Exception?, code: Int?, message: String?): Boolean {
    if (e is SocketTimeoutException) return true
    if (e is java.net.ConnectException) return true
    if (e is java.net.SocketException) return true

    if(message?.contains("Temporary error", ignoreCase = true) == true) return true

    if (code != null) {
        return when (code) {
            429,
            in 500..599 -> true
            else -> false
        }
    }

    if (message?.contains("timeout", ignoreCase = true) == true) return true
    return false
}


public fun now() = System.currentTimeMillis()
