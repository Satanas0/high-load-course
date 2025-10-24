package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    bucketSize: Int,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val queue = LinkedBlockingQueue<Request>(bucketSize)
    private val processingCount = AtomicInteger(0)
    private val maxConcurrentProcessing = rate.toInt()

    data class Request(
        val timestamp: Long = System.currentTimeMillis()
    )

    override fun tick(): Boolean {
        val request = Request()
        val offered = queue.offer(request)
        if (offered) {
            logger.trace("Request queued. Queue size: ${queue.size}")
        } else {
            logger.trace("Queue full, rejecting request")
        }
        return offered
    }

    fun tickWithTimeout(timeout: Long, unit: TimeUnit): Boolean {
        val request = Request()
        return try {
            queue.offer(request, timeout, unit)
        } catch (e: InterruptedException) {
            false
        }
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val delayMillis = window.toMillis() / rate
            delay(delayMillis)
            
            if (processingCount.get() < maxConcurrentProcessing) {
                val request = queue.poll()
                if (request != null) {
                    processingCount.incrementAndGet()
                    rateLimiterScope.launch {
                        delay(100)
                        processingCount.decrementAndGet()
                    }
                }
            }
        }
    }.invokeOnCompletion { th -> 
        if (th != null) logger.error("Rate limiter release job completed", th) 
    }

    fun queueSize(): Int = queue.size

    fun availableCapacity(): Int = queue.remainingCapacity()

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }
}
