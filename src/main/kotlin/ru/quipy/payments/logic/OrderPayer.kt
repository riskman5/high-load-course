package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.*
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.ScheduledThreadPoolExecutor
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

    private val paymentExecutor = object : ScheduledThreadPoolExecutor(
        5000,
        NamedThreadFactory("payment-submission-executor")
    ) {
        init {
            setMaximumPoolSize(5000)
            setKeepAliveTime(0L, TimeUnit.MILLISECONDS)
            setRejectedExecutionHandler(CallerBlockingRejectedExecutionHandler())
            removeOnCancelPolicy = true
        }
    }

    private val bucketQueue = LeakingBucketRateLimiter(rate = 2000, window = Duration.ofMillis(1000), bucketSize = 8000)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()

        if (!bucketQueue.tick()) {
            return null
        }

        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(paymentId, orderId, amount)
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            retryAsync(paymentId, amount, createdAt, deadline, attempt = 1)
        }

        return createdAt
    }

    private fun retryAsync(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val now = System.currentTimeMillis()
        val timeLeft = deadline - now

        if (timeLeft <= 0) {
            logger.warn("Payment $paymentId attempt #$attempt aborted: deadline exceeded")
            return
        }

        val paymentRequest = paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        val start = System.currentTimeMillis()

        paymentRequest
            .orTimeout(timeLeft, TimeUnit.MILLISECONDS)
            .whenCompleteAsync({ success, error ->
                System.currentTimeMillis() - start

                when {
                    error != null -> {

                        logger.warn(
                            "Payment $paymentId attempt #$attempt failed: ${error.message}, " +
                                    "timeLeft=${deadline - System.currentTimeMillis()}ms"
                        )

                        scheduleRetry(paymentId, amount, createdAt, deadline, attempt)
                    }

                    success == true -> {
                        logger.info("Payment $paymentId attempt #$attempt succeeded")
                    }

                    else -> {
                        logger.info("Payment $paymentId attempt #$attempt returned failure")

                        scheduleRetry(paymentId, amount, createdAt, deadline, attempt)
                    }
                }
            }, paymentExecutor)
    }

    private fun scheduleRetry(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val now = System.currentTimeMillis()
        val timeLeft = deadline - now

        if (timeLeft <= 0) {
            return
        }

        paymentExecutor.schedule(
            {
                retryAsync(paymentId, amount, createdAt, deadline, attempt + 1)
            },
            100L,
            TimeUnit.MILLISECONDS
        )
    }
}