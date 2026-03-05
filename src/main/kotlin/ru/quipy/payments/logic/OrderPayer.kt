package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val rateLimiter = TokenBucketRateLimiter(
        5000,
        5000,
        1,
        TimeUnit.SECONDS
    )

    private val coroutineScope = CoroutineScope(Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher())

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()

        if (deadline <= createdAt) {
//            logger.warn("Payment $paymentId rejected: deadline already passed")
            return null
        }

        if (!rateLimiter.tick()) {
//            logger.warn("Payment $paymentId rejected: rate limit reached")
            return null
        }

        coroutineScope.launch {
            try {
                launch {
                    paymentESService.create {
                        it.create(paymentId, orderId, amount)
                    }
                }

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            } catch (e: Exception) {
                logger.error("Payment failed unexpectedly: $paymentId", e)
            }
        }

        return createdAt
    }
}