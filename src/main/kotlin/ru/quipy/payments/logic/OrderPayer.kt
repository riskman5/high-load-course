package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)

        private const val MAX_IN_FLIGHT_PAYMENTS = 5000
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val backpressureSemaphore = Semaphore(MAX_IN_FLIGHT_PAYMENTS)

    private val coroutineScope = CoroutineScope(Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher())

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()

        backpressureSemaphore.acquire()

        coroutineScope.launch {
            try {
                val createdEvent = paymentESService.create {
                    it.create(paymentId, orderId, amount)
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            } catch (e: Exception) {
                logger.error("Payment failed unexpectedly: $paymentId", e)
            } finally {
                backpressureSemaphore.release()
            }
        }

        return createdAt
    }
}