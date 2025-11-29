package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.domain.Event
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .callTimeout(2000, TimeUnit.MILLISECONDS)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )
    
    private val ongoingWindow = OngoingWindow(parallelRequests)
    
    private val incomingRequestsCounter = Counter.builder("payment_requests_incoming")
        .tag("accountName", accountName)
        .description("Incoming payment requests count")
        .register(Metrics.globalRegistry)
        
    private val completedRequestsCounter = Counter.builder("payment_requests_completed")
        .tag("accountName", accountName)
        .description("Completed payment requests count")
        .register(Metrics.globalRegistry)
        
    private val expiredRequestsCounter = Counter.builder("payment_requests_expired")
        .tag("accountName", accountName)
        .description("Expired payment requests count")
        .register(Metrics.globalRegistry)

    private val eventStoreQueue =
        LinkedBlockingQueue<(EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>) -> Unit>(500_000)

    @Suppress("unused")
    private val eventStoreExecutor = Executors.newFixedThreadPool(20).also { executor ->
        repeat(20) {
            executor.submit {
                while (true) {
                    val task = eventStoreQueue.take()
                    try {
                        task(paymentESService)
                    } catch (e: Exception) {
                        logger.error("[$accountName] EventStore update failed", e)
                    }
                }
            }
        }
    }

    private fun queueEventStoreUpdate(
        paymentId: UUID,
        block: (PaymentAggregateState) -> Event<PaymentAggregate>
    ) {
        eventStoreQueue.put { es ->
            es.update(paymentId) { state ->
                block(state)
            }
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        incomingRequestsCounter.increment()
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        ongoingWindow.acquire()

        val sample = Timer.start()

        try {
            val transactionId = UUID.randomUUID()

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            queueEventStoreUpdate(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            var attempt = 0
            val maxRetries = 2
            var sleepTime = 1000L
            
            while (attempt < maxRetries) {
                if (System.currentTimeMillis() > deadline) {
                    logger.warn("[$accountName] Payment $paymentId deadline exceeded")
                    queueEventStoreUpdate(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
                    }
                    return
                }
                
                while (!rateLimiter.tick()) {
                    if (System.currentTimeMillis() > deadline) {
                        logger.warn("[$accountName] Payment $paymentId expired while waiting for rate limit")
                        queueEventStoreUpdate(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded while waiting for rate limit")
                        }
                        expiredRequestsCounter.increment()
                        return
                    }
                }
                
                try {
                    val request = Request.Builder().run {
                        url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                        post(emptyBody)
                    }.build()

                    client.newCall(request).execute().use { response ->
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                        if (!body.result && attempt < maxRetries - 1) {
                            if (System.currentTimeMillis() + sleepTime > deadline) {
                                logger.warn("[$accountName] Payment $paymentId no time for retry, deadline too close")
                                queueEventStoreUpdate(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = body.message)
                                }
                                return
                            }
                            attempt++
                            logger.warn("[$accountName] Payment $paymentId failed, retry $attempt/$maxRetries, sleep ${sleepTime}ms")
                            Thread.sleep(sleepTime)
                            sleepTime += 1000L
                            return@use
                        }

                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        queueEventStoreUpdate(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                        return
                    }
                } catch (e: InterruptedIOException) {
                    logger.warn("[$accountName] Timeout/interrupted during payment $paymentId, will retry if attempts left", e)
                    attempt++
                    Thread.sleep(sleepTime)
                    sleepTime += 1000L
                    continue
                } catch (e: Exception) {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    queueEventStoreUpdate(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                    return
                }
            }
            
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId after $maxRetries attempts")
            queueEventStoreUpdate(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Failed after retries")
            }
        } finally {
            sample.stop(Metrics.timer(
                "payment_duration_seconds",
                "service", serviceName,
                "account", accountName
            ))

            completedRequestsCounter.increment()
            ongoingWindow.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()