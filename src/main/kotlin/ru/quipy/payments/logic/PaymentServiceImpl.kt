package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.CompletableFuture


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) : CompletableFuture<Void> {
        val futures = paymentAccounts.map { account ->
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }

        return CompletableFuture.allOf(*futures.toTypedArray())
    }
}