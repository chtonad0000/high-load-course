package ru.quipy.payments.logic

import kotlinx.coroutines.Job
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.LinkedList
import java.util.UUID

@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(orderId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): List<Job> {
        val jobs = LinkedList<Job>()
        for (account in paymentAccounts) {
            jobs.add(account.performPaymentAsync(orderId, paymentId, amount, paymentStartedAt, deadline))
        }
        return jobs
    }
}
