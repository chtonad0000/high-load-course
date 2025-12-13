package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.domain.Event
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

// ВАЖНО: никаких внутренних очередей/циклов ожидания rateLimiter здесь больше нет.
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper: ObjectMapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime  // PT10S
    private val rateLimitPerSec = properties.rateLimitPerSec                     // 1100
    private val parallelRequests = properties.parallelRequests                   // 20000

    // Async HTTP-клиент без блокировок
    private val httpClient: HttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofSeconds(2))
        .build()

    // rps-лимит: используем только одноразовый tick(), без ожиданий
    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    // ограничение числа одновременных запросов к внешней системе
    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)

    // отдельный пул для обновлений EventStore, как в исходнике
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

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        incomingRequestsCounter.increment()

        val transactionId = UUID.randomUUID()
        val now = now()

        fun failFast(reason: String) {
            // логируем "submission" как неуспешный — тесту видно, что мы заявку видели, но не отправили во внешнюю систему
            queueEventStoreUpdate(paymentId) {
                it.logSubmission(
                    false,
                    transactionId,
                    now(),
                    Duration.ofMillis(now() - paymentStartedAt)
                )
            }
            queueEventStoreUpdate(paymentId) {
                it.logProcessing(false, now(), transactionId, reason)
            }
            expiredRequestsCounter.increment()
        }

        // если уже заведомо не уложимся в дедлайн — сразу отказ
        if (now + requestAverageProcessingTime.toMillis() > deadline) {
            logger.warn("[$accountName] Payment $paymentId deadline too close, reject immediately")
            failFast("Deadline too close for processing")
            return
        }

        // ограничиваем параллелизм НЕ блокируя поток
        val winRes = ongoingWindow.putIntoWindow()
        if (winRes is NonBlockingOngoingWindow.WindowResponse.Fail) {
            logger.warn("[$accountName] Too many parallel requests, reject payment $paymentId")
            failFast("Too many parallel requests")
            return
        }

        // одноразовый rate-лимит: либо пропускаем, либо сразу отказываем, без ожиданий и очередей
        if (!rateLimiter.tick()) {
            logger.warn("[$accountName] Rate limit exceeded, reject payment $paymentId")
            ongoingWindow.releaseWindow()
            failFast("Rate limit per second exceeded")
            return
        }

        // с этого момента считаем, что запрос реально "отправляем" во внешнюю систему
        queueEventStoreUpdate(paymentId) {
            it.logSubmission(
                true,
                transactionId,
                now(),
                Duration.ofMillis(now() - paymentStartedAt)
            )
        }

        val timerSample = Timer.start()

        val remaining = deadline - now()
        val timeoutMillis = remaining
            .coerceAtLeast(requestAverageProcessingTime.plusSeconds(5).toMillis())
            .coerceAtMost(60_000L) // не даём висеть бесконечно

        val host = paymentProviderHostPort.substringBefore(':')
        val port = paymentProviderHostPort.substringAfter(':').toInt()

        val uri = URI(
            "http",
            null,
            host,
            port,
            "/external/process",
            "serviceName=$serviceName" +
                    "&token=$token" +
                    "&accountName=$accountName" +
                    "&transactionId=$transactionId" +
                    "&paymentId=$paymentId" +
                    "&amount=$amount",
            null
        )

        val request = HttpRequest.newBuilder()
            .uri(uri)
            .timeout(Duration.ofMillis(timeoutMillis))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .whenComplete { response, error ->
                try {
                    if (error != null) {
                        logger.warn("[$accountName] HTTP error for payment $paymentId", error)
                        queueEventStoreUpdate(paymentId) {
                            it.logProcessing(false, now(), transactionId, error.message)
                        }
                    } else {
                        val bodyStr = response!!.body()
                        val body = try {
                            mapper.readValue(bodyStr, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error(
                                "[$accountName] Failed to parse response for payment $paymentId, body=$bodyStr",
                                e
                            )
                            ExternalSysResponse(
                                transactionId.toString(),
                                paymentId.toString(),
                                false,
                                e.message
                            )
                        }

                        logger.warn(
                            "[$accountName] Payment processed for txId=$transactionId, payment=$paymentId, " +
                                    "result=${body.result}, msg=${body.message}"
                        )

                        queueEventStoreUpdate(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, body.message)
                        }
                    }
                } finally {
                    timerSample.stop(
                        Metrics.timer(
                            "payment_duration_seconds",
                            "service", serviceName,
                            "account", accountName
                        )
                    )
                    completedRequestsCounter.increment()
                    ongoingWindow.releaseWindow()
                }
            }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
