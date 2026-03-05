package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val sum = AtomicLong(0)
    private val queue = ConcurrentLinkedQueue<Long>()

    override fun tick(): Boolean {
        while (true) {
            val curSum = sum.get()
            if (curSum >= rate) return false
            if (sum.compareAndSet(curSum, curSum + 1)) {
                queue.add(System.currentTimeMillis())
                return true
            }
        }
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

    fun tickBlocking(timeout: Duration): Boolean {
        val deadlineNanos = System.nanoTime() + timeout.toNanos()
        while (!tick()) {
            if (System.nanoTime() >= deadlineNanos) return false
            Thread.sleep(10)
        }
        return true
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val winStart = System.currentTimeMillis() - window.toMillis()
            var released = 0
            while (true) {
                val head = queue.peek() ?: break
                if (head > winStart) break
                queue.poll()
                released++
            }
            if (released > 0) sum.addAndGet(-released.toLong())
            delay(1L)
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}
