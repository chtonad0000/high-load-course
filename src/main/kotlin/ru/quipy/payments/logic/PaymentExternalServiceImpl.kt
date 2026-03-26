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
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
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
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


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
        private const val DEADLINE_BUFFER_MS = 100L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val retryAmount = 2
    private val hedgeDelayMs = 100L
    private val requestTimeoutMs = 1400L

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong())
    private val semaphore = Semaphore(parallelRequests)
    private val scheduler = Executors.newScheduledThreadPool(100)

    private val http2Client = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(128))
        .version(HttpClient.Version.HTTP_2)
        .build()

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
        newFixedThreadPoolContext(250, "payment_pool") + SupervisorJob()
    )

    @OptIn(DelicateCoroutinesApi::class)
    private val dbScope = CoroutineScope(
        newFixedThreadPoolContext(100, "db_pool")
    )

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

            val idempotencyKey = transactionId.toString()
            fun createRequest(): HttpRequest = HttpRequest.newBuilder()
                .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
                .header("x-idempotency-key", idempotencyKey)
                .timeout(Duration.ofMillis(requestTimeoutMs))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()

            val futures = mutableListOf<CompletableFuture<HttpResponse<String>>>()
            futures.add(http2Client.sendAsync(createRequest(), HttpResponse.BodyHandlers.ofString()))

            for (i in 1..retryAmount) {
                scheduler.schedule({
                    if (futures.any { it.isDone }) return@schedule
                    futures.add(http2Client.sendAsync(createRequest(), HttpResponse.BodyHandlers.ofString()))
                }, hedgeDelayMs * i, TimeUnit.MILLISECONDS)
            }

            @Suppress("UNCHECKED_CAST")
            val response = CompletableFuture.anyOf(*futures.toTypedArray()).await() as HttpResponse<String>

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }

            if (body.result) {
                completedRequestsCounter.increment()
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
