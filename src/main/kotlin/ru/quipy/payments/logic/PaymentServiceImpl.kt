package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val adapters: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        private val logger = LoggerFactory.getLogger(PaymentService::class.java)
    }

    private val activeAdapters: List<PaymentExternalSystemAdapter> =
        adapters.filter { it.isEnabled() }.sortedBy { it.price() }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val adapter = activeAdapters.firstOrNull()
        if (adapter == null) {
            logger.error("No enabled payment accounts available")
            return
        }
        adapter.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
    }
}