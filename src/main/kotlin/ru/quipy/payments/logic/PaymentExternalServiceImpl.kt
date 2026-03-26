package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.WebClient
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val webClient: WebClient
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper: ObjectMapper = ObjectMapper().registerKotlinModule()
        private const val DEADLINE_BUFFER_MS = 150L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter((rateLimitPerSec * 0.95).toLong())
    private val semaphore = Semaphore(parallelRequests)

    private val incomingRequestsCounter = Counter.builder("payment_requests_incoming")
        .tag("accountName", accountName)
        .description("Incoming payment requests count")
        .register(Metrics.globalRegistry)

    private val completedRequestsCounter = Counter.builder("payment_requests_completed")
        .tag("accountName", accountName)
        .description("Completed payment requests count")
        .register(Metrics.globalRegistry)

    @OptIn(DelicateCoroutinesApi::class)
    private val paymentScope = CoroutineScope(
        newFixedThreadPoolContext(200, "payment_pool") + SupervisorJob()
    )

    @OptIn(DelicateCoroutinesApi::class)
    private val dbScope = CoroutineScope(
        newFixedThreadPoolContext(100, "db_pool")
    )

    override fun performPaymentAsync(orderId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): Job {
        incomingRequestsCounter.increment()
        return paymentScope.launch {
            executePayment(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    private suspend fun executePayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        val sample = Timer.start()

        dbScope.launch {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        val remainingBeforeRequest = deadline - now() - DEADLINE_BUFFER_MS
        if (remainingBeforeRequest <= 0) {
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline too close on entry.")
                }
            }
            return
        }

        if (!semaphore.tryAcquire()) {
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Too many parallel requests.")
                }
            }
            return
        }

        try {
            val rateLimitTimeout = deadline - now() - DEADLINE_BUFFER_MS
            if (rateLimitTimeout <= 0 || !rateLimiter.tickSuspend(rateLimitTimeout)) {
                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Rate limiter timeout.")
                    }
                }
                return
            }

            val timeoutMillis = deadline - now() - DEADLINE_BUFFER_MS
            if (timeoutMillis <= 0) {
                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "No time left after rate limiting.")
                    }
                }
                return
            }

            val response = webClient
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
                .timeout(Duration.ofMillis(timeoutMillis))
                .awaitSingleOrNull()

            if (response != null) {
                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(
                            response.body!!.result, now(), transactionId, reason = response.body!!.message
                        )
                    }
                }
                completedRequestsCounter.increment()
            } else {
                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Empty response (timeout or error).")
                    }
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    dbScope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }
                }
                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    dbScope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            }
        } finally {
            semaphore.release()
            sample.stop(Metrics.timer(
                "payment_duration_seconds",
                "service", serviceName,
                "account", accountName
            ))
        }
    }

    fun preWarmConnection() {
        try {
            logger.info("[$accountName] Pre-warming connection to $paymentProviderHostPort")
            for (i in 1..3) {
                webClient
                    .get()
                    .uri("http://$paymentProviderHostPort/external/accounts?serviceName=$serviceName&token=$token")
                    .retrieve()
                    .toBodilessEntity()
                    .block(Duration.ofSeconds(5))
            }
            logger.info("[$accountName] Connection pre-warmed successfully")
        } catch (e: Exception) {
            logger.warn("[$accountName] Connection pre-warm failed: ${e.message}")
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
