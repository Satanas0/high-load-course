package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.Logger
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.WebClient
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.metrics.MetricsService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.*
import java.util.concurrent.Semaphore


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metricsService: MetricsService,
    private val webClient: WebClient
) : PaymentExternalSystemAdapter {

    companion object {
        val emptyBody = ByteArray(0).toRequestBody(null)
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
        private const val TASK_NAME = "paymentTask"
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(properties.parallelRequests)

    private val timeout = Duration.ofMillis(1090)
    private val client = OkHttpClient.Builder()
        .callTimeout(timeout)
        .connectTimeout(timeout)
        .readTimeout(timeout)
        .writeTimeout(timeout)
        .build()

    @OptIn(DelicateCoroutinesApi::class)
    private val dbScope = CoroutineScope(newFixedThreadPoolContext(100, "db_pool"))

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        val start = System.currentTimeMillis()
        val avgMs = requestAverageProcessingTime.toMillis()
        val remaining = deadline - start
        val maxRetries = 5
        val timeout = 500L

        logger.info("[$accountName] Submitting payment $paymentId (txId=$transactionId), remaining=${remaining}ms")

        dbScope.launch {
            paymentESService.update(paymentId) {
                it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        // Always record submission
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(start - paymentStartedAt))
        }

        if (remaining < avgMs) {
            logger.warn("[$accountName] Skipping payment $paymentId: remaining ${remaining}ms < avg ${avgMs}ms")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline too close")
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

            if (timeLeft < avgMs) {
                logger.warn("[$accountName] Stop retrying payment $paymentId: deadline too close (${timeLeft}ms left)")
                break
            }

            semaphore.acquire()
            if (!rateLimiter.tick()) {
                val retryDelay = timeout * attempt
                logger.warn("[$accountName] Rate limited (attempt $attempt) delaying $retryDelay ms")
                semaphore.release()
                Thread.sleep(retryDelay)
                continue
            }

        try {
            val response = semaphore.withPermit {
                rateLimiter.tickBlocking()
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
                    .awaitSingle()
            }
            try {
                val request = Request.Builder()
                    .url(
                        "http://$paymentProviderHostPort/external/process" +
                                "?serviceName=$serviceName" +
                                "&token=$token" +
                                "&accountName=$accountName" +
                                "&transactionId=$transactionId" +
                                "&paymentId=$paymentId" +
                                "&amount=$amount"
                    )
                    .post(emptyBody)
                    .build()

                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Failed to parse response: ${e.message}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(response.body!!.result, now(), transactionId, reason = response.body!!.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    dbScope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }
                }
                else -> {
                    dbScope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            }
        }
    }
                    logger.info("CODE: ${response.code}")


                    val duration = System.currentTimeMillis() - attemptStart
                    metricsService.recordRequestDuration(duration, body.result)

                    if (response.isSuccessful && body.result) {
                        logger.info("[$accountName] Payment success for txId=$transactionId, payment=$paymentId")
                        success = true
                        paymentESService.update(paymentId) {
                            it.logProcessing(true, now(), transactionId, reason = body.message)
                        }
                    } else {
                        val retriable = isRetriable(null, response.code, body.message)
                        lastReason = "HTTP ${response.code}: ${body.message}"
                        logger.warn("[$accountName] Payment failed (attempt $attempt/$maxRetries): $lastReason, retriable=$retriable")

                        if (!retriable) break

                        val backoff = (timeout * attempt).coerceAtMost(timeLeft / 2)
                        Thread.sleep(backoff)
                    }
                }
                metricsService.incrementCompletedTask(TASK_NAME)
            } catch (e: Exception) {
                val retriable = isRetriable(e, null, e.message)
                lastReason = e.message
                logger.error("[$accountName] Exception during payment $paymentId: ${e.javaClass.simpleName}, retriable=$retriable", e)

                if (!retriable) break

                val backoff = (timeout * attempt).coerceAtMost((deadline - now()) / 2)
                Thread.sleep(backoff)
            } finally {
                semaphore.release()
            }
        }

        if (!success) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = lastReason ?: "Permanent failure")
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
