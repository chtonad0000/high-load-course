package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

class LeakingBucketRateLimiter(
    private val ratePerSecond: Long,
    bucketSize: Int,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val queue = LinkedBlockingQueue<Int>(bucketSize)
    
    private val intervalMs = 10L
    private val releasePerInterval = (ratePerSecond * intervalMs / 1000).coerceAtLeast(1)

    override fun tick(): Boolean {
        return queue.offer(1)
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            delay(intervalMs)
            repeat(releasePerInterval.toInt()) {
                queue.poll()
            }
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }
}