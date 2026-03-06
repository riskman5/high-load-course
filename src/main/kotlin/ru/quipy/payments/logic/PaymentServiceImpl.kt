package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    private val sortedAccounts = paymentAccounts
        .filter { it.isEnabled() }
        .sortedBy { it.price() }

    override suspend fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val account = sortedAccounts.firstOrNull()
        if (account == null) {
            logger.error("No enabled payment accounts available for payment $paymentId")
            return
        }
        account.performPayment(paymentId, amount, paymentStartedAt, deadline)
    }
}