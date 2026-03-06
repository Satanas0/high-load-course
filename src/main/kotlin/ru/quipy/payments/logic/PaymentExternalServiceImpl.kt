package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.metrics.MetricsService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max

class PaymentExternalSystemAdapterImpl(
    val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metricsService: MetricsService,
    private val dbScope: CoroutineScope,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val MAX_ATTEMPTS = 5

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime.toMillis()
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong() / 10, Duration.ofMillis(100))
    private val ongoingWindow = OngoingWindow(parallelRequests, false)
    private val latencyProfile = RollingLatencyProfile(maxSize = 2048, fallbackMs = requestAverageProcessingTime.coerceAtLeast(20L))

    private val httpThreadPoolSize = maxOf(100, parallelRequests / 10)
    private val httpExecutor = ThreadPoolExecutor(
        httpThreadPoolSize,
        httpThreadPoolSize,
        60L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(parallelRequests * 2),
        Executors.defaultThreadFactory(),
        CallerBlockingRejectedExecutionHandler(Duration.ofSeconds(5))
    )

    private val client: HttpClient = HttpClient.newBuilder()
        .executor(httpExecutor)
        .connectTimeout(Duration.ofMillis(1000L))
        .version(HttpClient.Version.HTTP_2)
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, orderId: UUID, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        val startedAt = now()
        dbScope.launch {
            while (true) {
                try {
                    paymentESService.update(paymentId) {
                        it.logSubmission(
                            success = true,
                            transactionId,
                            startedAt,
                            Duration.ofMillis(startedAt - paymentStartedAt)
                        )
                    }
                    break
                } catch (_: IllegalArgumentException) {
                    delay(10)
                }
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
        performPaymentAsync(paymentId, amount, paymentStartedAt, deadline, transactionId, 0)
    }

    private fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, transactionId: UUID, attempt: Long) {
        if (now() + requestAverageProcessingTime > deadline || attempt >= MAX_ATTEMPTS) {
            metricsService.incrementCounter("payment_failed_external", "Failed external requests")
            val currentTime = now()
            dbScope.launch {
                while (true) {
                    try {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, currentTime, transactionId, reason = "Deadline exceeded or max attempts reached")
                        }
                        break
                    } catch (_: IllegalArgumentException) {
                        delay(10)
                    }
                }
            }
            return
        }

        if (!rateLimiter.tickBlocking(Duration.ofMillis(deadline - now()))) {
            metricsService.incrementCounter("payment_ratelimit_reject", "Rate limiter rejections")
            val currentTime = now()
            dbScope.launch {
                while (true) {
                    try {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, currentTime, transactionId, reason = "Rate limit exceed")
                        }
                        break
                    } catch (_: IllegalArgumentException) {
                        delay(10)
                    }
                }
            }
            return
        }

        val timeToBlock = deadline - System.currentTimeMillis()
        val acquired = ongoingWindow.tryAcquire(timeToBlock, TimeUnit.MILLISECONDS)
        if (!acquired) {
            logger.warn("[$accountName] Timeout acquiring semaphore for payment $paymentId")
            metricsService.incrementCounter("payment_semaphore_timeout", "Semaphore timeouts")
            val currentTime = now()
            dbScope.launch {
                while (true) {
                    try {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, currentTime, transactionId, reason = "Semaphore timeout")
                        }
                        break
                    } catch (_: IllegalArgumentException) {
                        delay(10)
                    }
                }
            }
            return
        }

        val callTimeout = computeCallTimeoutMs(deadline - now())
        val request = HttpRequest
            .newBuilder()
            .timeout(Duration.ofMillis(callTimeout))
            .header("deadline", "$deadline")
            .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val attemptStart = now()
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->
            val duration = now() - attemptStart
            latencyProfile.record(duration.coerceAtLeast(1))

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }
            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

            val currentTime = now()
            val result = body.result
            val message = body.message
            dbScope.launch {
                while (true) {
                    try {
                        paymentESService.update(paymentId) {
                            it.logProcessing(result, currentTime, transactionId, reason = message)
                        }
                        break
                    } catch (_: IllegalArgumentException) {
                        delay(10)
                    }
                }
            }

            if (body.result) {
                metricsService.incrementCounter("payment_success", "Successful payments")
                ongoingWindow.release()
            }
            else {
                metricsService.increaseRetryCounter()
                ongoingWindow.release()

                performPaymentAsync(paymentId, amount, paymentStartedAt, deadline, transactionId, attempt + 1)
            }
        }.exceptionally { ex ->
            val willRetry = attempt + 1 < MAX_ATTEMPTS
            when (ex) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", ex)
                    metricsService.incrementCounter("payment_failed_external", "Failed external requests")
                }
                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", ex)
                    metricsService.incrementCounter("payment_failed_external", "Failed external requests")
                }
            }
            ongoingWindow.release()

            if (willRetry) {
                metricsService.increaseRetryCounter()
                performPaymentAsync(paymentId, amount, paymentStartedAt, deadline, transactionId, attempt + 1)
            } else {
                val currentTime = now()
                val reason = if (ex is SocketTimeoutException) "Request timeout." else ex.message
                dbScope.launch {
                    while (true) {
                        try {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, currentTime, transactionId, reason = reason)
                            }
                            break
                        } catch (_: IllegalArgumentException) {
                            delay(10)
                        }
                    }
                }
            }
            null
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun maxRateLimit() = properties.rateLimitPerSec

    private fun computeCallTimeoutMs(timeLeft: Long): Long {
        val p95 = latencyProfile.quantile(0.95)
        val avg = requestAverageProcessingTime.coerceAtLeast(20L)
        val target = max(avg, p95)
        val adaptive = target.coerceAtLeast(150L).coerceAtMost(800L)
        val boundedByDeadline = (timeLeft - 50L).coerceAtLeast(500L)
        return adaptive.coerceAtMost(boundedByDeadline)
    }
}

private class RollingLatencyProfile(
    private val maxSize: Int,
    private val fallbackMs: Long,
    private val refreshInterval: Int = 64,
) {
    private val samples = ConcurrentLinkedDeque<Long>()
    private val recordCount = AtomicInteger(0)
    @Volatile private var cachedSorted: LongArray = LongArray(0)

    fun record(durationMs: Long) {
        samples.addLast(durationMs.coerceAtLeast(1L))
        while (samples.size > maxSize) {
            samples.pollFirst()
        }
        if (recordCount.incrementAndGet() % refreshInterval == 0) {
            cachedSorted = samples.toList().toLongArray().also { it.sort() }
        }
    }

    fun quantile(q: Double): Long {
        val sorted = cachedSorted
        if (sorted.isEmpty()) return fallbackMs
        val index = ((sorted.size - 1) * q).toInt().coerceIn(0, sorted.lastIndex)
        return sorted[index]
    }
}

public fun now() = System.currentTimeMillis()
