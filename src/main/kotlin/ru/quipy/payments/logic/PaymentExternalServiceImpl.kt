package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import org.springframework.web.reactive.function.client.WebClient
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


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
        private const val DEADLINE_BUFFER_MS = 100L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val requestTimeoutMs = 1500L

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong())
    private val semaphore = kotlinx.coroutines.sync.Semaphore(parallelRequests)

    private val circuitBreakerConfig = CircuitBreakerConfig.custom()
        .failureRateThreshold(50F)
        .slowCallRateThreshold(80F)
        .slowCallDurationThreshold(Duration.ofMillis(requestTimeoutMs))
        .waitDurationInOpenState(Duration.ofSeconds(3))
        .permittedNumberOfCallsInHalfOpenState(5)
        .minimumNumberOfCalls(10)
        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
        .slidingWindowSize(50)
        .build()

    private val circuitBreaker: CircuitBreaker = CircuitBreakerRegistry.of(circuitBreakerConfig)
        .circuitBreaker("payment-$accountName")

    private val http2Client = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(128))
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val incomingRequestsCounter = Counter.builder("payment_requests_incoming")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    private val completedRequestsCounter = Counter.builder("payment_requests_completed")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    private val circuitBreakerRejectedCounter = Counter.builder("payment_circuit_breaker_rejected")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    private val paymentScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val dbScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private fun recordSuccess(durationMs: Long) {
        circuitBreaker.onSuccess(durationMs, TimeUnit.MILLISECONDS)
    }

    private fun recordError(durationMs: Long, throwable: Throwable) {
        circuitBreaker.onError(durationMs, TimeUnit.MILLISECONDS, throwable)
    }

    override fun performPaymentAsync(orderId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): Job {
        incomingRequestsCounter.increment()
        return paymentScope.launch {
            executePayment(orderId, paymentId, amount, paymentStartedAt, deadline)
        }
    }

    private suspend fun executePayment(orderId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        val sample = Timer.start()

        dbScope.launch {
            try {
                paymentESService.create {
                    it.create(paymentId, orderId, amount)
                }
            } catch (_: Exception) { }
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

        if (!circuitBreaker.tryAcquirePermission()) {
            circuitBreakerRejectedCounter.increment()
            logger.warn("[$accountName] Circuit breaker OPEN, rejecting payment $paymentId")
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Circuit breaker is OPEN.")
                }
            }
            return
        }
        val cbStartTime = now()

        if (!semaphore.tryAcquire()) {
            recordError(now() - cbStartTime, RuntimeException("Too many parallel requests"))
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Too many parallel requests.")
                }
            }
            return
        }

        try {
            val maxRateLimitWait = minOf(deadline - now() - DEADLINE_BUFFER_MS, 2000L)
            if (maxRateLimitWait <= 0 || !rateLimiter.tickSuspend(maxRateLimitWait)) {
                recordError(now() - cbStartTime, RuntimeException("Rate limiter timeout"))
                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Rate limiter timeout.")
                    }
                }
                return
            }

            val timeoutMillis = deadline - now() - DEADLINE_BUFFER_MS
            if (timeoutMillis <= 0) {
                recordError(now() - cbStartTime, RuntimeException("No time left"))
                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "No time left after rate limiting.")
                    }
                }
                return
            }

            val idempotencyKey = transactionId.toString()
            val request = HttpRequest.newBuilder()
                .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
                .header("x-idempotency-key", idempotencyKey)
                .timeout(Duration.ofMillis(minOf(requestTimeoutMs, timeoutMillis)))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()

            val response = try {
                withTimeout(timeoutMillis) {
                    http2Client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
                }
            } catch (e: kotlinx.coroutines.TimeoutCancellationException) {
                logger.warn("[$accountName] Payment timed out for txId: $transactionId, payment: $paymentId")
                recordError(now() - cbStartTime, e)
                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline timeout.")
                    }
                }
                return
            }

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment for txId: $transactionId, payment: $paymentId, code: ${response.statusCode()}, body: ${response.body()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            logger.warn("[$accountName] Payment for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

            val duration = now() - cbStartTime
            if (body.result) {
                recordSuccess(duration)
                completedRequestsCounter.increment()
            } else {
                recordError(duration, RuntimeException(body.message ?: "Payment failed"))
            }

            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            val duration = now() - cbStartTime
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    recordError(duration, e)
                    dbScope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }
                }
                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    recordError(duration, e)
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
            reactor.core.publisher.Flux.range(1, 8000)
                .flatMap({
                    webClient
                        .get()
                        .uri("http://$paymentProviderHostPort/external/accounts?serviceName=$serviceName&token=$token")
                        .retrieve()
                        .toBodilessEntity()
                }, 1000)
                .collectList()
                .block(Duration.ofSeconds(120))
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
