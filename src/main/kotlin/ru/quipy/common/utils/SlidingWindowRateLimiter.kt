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
    private val window: Duration = Duration.ofSeconds(1),
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val sum = AtomicLong(0)
    private val queue = ConcurrentLinkedQueue<Long>()

    override fun tick(): Boolean {
        while (true) {
            val curSum = sum.get()
            if (curSum >= rate) return false
            if (sum.compareAndSet(curSum, curSum + 1)) {
                queue.add(System.nanoTime())
                return true
            }
        }
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

    suspend fun tickSuspend(timeoutMillis: Long = Long.MAX_VALUE): Boolean {
        val start = System.currentTimeMillis()
        while (!tick()) {
            if (System.currentTimeMillis() - start >= timeoutMillis) return false
            delay(1L)
        }
        return true
    }

    private val releaseJob = rateLimiterScope.launch {
        val windowNanos = window.toNanos()
        while (true) {
            val head = queue.peek()
            if (head == null) {
                delay(1L)
                continue
            }
            val now = System.nanoTime()
            val elapsed = now - head
            if (elapsed >= windowNanos) {
                queue.poll()
                sum.decrementAndGet()
            } else {
                val remainingMs = (windowNanos - elapsed) / 1_000_000
                if (remainingMs > 1) {
                    delay(remainingMs)
                } else {
                    delay(1L)
                }
            }
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}
