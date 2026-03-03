package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()

        const val HEDGE_DELAY_FRACTION = 0.7
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec

    private val hedgeDelayMs = (requestAverageProcessingTime.toMillis() * HEDGE_DELAY_FRACTION).toLong()

    private val client: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(1500))
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        (rateLimitPerSec * 0.95).toLong().coerceAtLeast(1),
        Duration.ofSeconds(1)
    )

    private val updateScope = CoroutineScope(Dispatchers.IO)

    private val incomingRequestsCounter: Counter = Counter
        .builder("incoming.requests")
        .tags("account", accountName)
        .register(meterRegistry)

    private val incomingFinishedRequestsCounter: Counter = Counter
        .builder("incoming.finished.requests")
        .tags("account", accountName)
        .register(meterRegistry)

    private val outgoingRequestsCounter: Counter = Counter
        .builder("outgoing.requests")
        .tags("account", accountName)
        .register(meterRegistry)

    private val outgoingFinishedRequestsCounter: Counter = Counter
        .builder("outgoing.finished.requests")
        .tags("account", accountName)
        .register(meterRegistry)

    private val clientRequestLatency: Timer = Timer
        .builder("client.request.latency")
        .publishPercentiles(0.5, 0.8, 0.9)
        .register(meterRegistry)


    override suspend fun performPayment(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        incomingRequestsCounter.increment()

        updateScope.launch {
            paymentESService.update(paymentId) {
                it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        performHedgedPayment(paymentId, amount, transactionId, deadline)
    }

    private suspend fun performHedgedPayment(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ) {
        if (remainingMillis(deadline) < requestAverageProcessingTime.toMillis()) {
            incomingFinishedRequestsCounter.increment()
            return
        }

        if (!slidingWindowRateLimiter.tickSuspend(
                remainingMillis(deadline) - requestAverageProcessingTime.toMillis(),
                TimeUnit.MILLISECONDS
            )
        ) {
            logger.error("[$accountName] Payment timeout (rate limiter) for txId: $transactionId, payment: $paymentId")
            updateScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Timeout - rateLimiter")
                }
            }
            incomingFinishedRequestsCounter.increment()
            return
        }

        try {
            coroutineScope {
                val firstRequest = async {
                    sendPaymentRequest(paymentId, amount, transactionId, deadline)
                }

                val hedgeRequest = async {
                    delay(hedgeDelayMs)
                    if (firstRequest.isCompleted) {
                        return@async null
                    }

                    if (remainingMillis(deadline) < requestAverageProcessingTime.toMillis()) {
                        return@async null
                    }

                    if (!slidingWindowRateLimiter.tickSuspend(
                            (remainingMillis(deadline) - requestAverageProcessingTime.toMillis())
                                .coerceAtLeast(1),
                            TimeUnit.MILLISECONDS
                        )
                    ) {
                        return@async null
                    }

                    logger.info("[$accountName] Hedged request triggered for txId: $transactionId, payment: $paymentId")
                    sendPaymentRequest(paymentId, amount, transactionId, deadline)
                }

                val result: ExternalSysResponse? = select {
                    firstRequest.onAwait { it }
                    hedgeRequest.onAwait { it }
                }

                firstRequest.cancel()
                hedgeRequest.cancel()

                if (result != null) {
                    logger.info(
                        "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${result.result}"
                    )
                    updateScope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(result.result, now(), transactionId, reason = result.message)
                        }
                    }
                } else {
                    updateScope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "No response from hedged requests")
                        }
                    }
                }

                incomingFinishedRequestsCounter.increment()
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Hedged payment failed for txId: $transactionId, payment: $paymentId", e)
            updateScope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }
            incomingFinishedRequestsCounter.increment()
        }
    }

    private suspend fun sendPaymentRequest(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ): ExternalSysResponse? {
        val timeoutMs = remainingMillis(deadline)
            .coerceAtMost(requestAverageProcessingTime.toMillis())
            .coerceAtLeast(1L)

        val uri = "http://$paymentProviderHostPort/external/process" +
                "?serviceName=$serviceName" +
                "&token=$token" +
                "&accountName=$accountName" +
                "&transactionId=$transactionId" +
                "&paymentId=$paymentId" +
                "&amount=$amount"

        val request = HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .timeout(Duration.ofMillis(timeoutMs))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val clientRequestStart = now()
        outgoingRequestsCounter.increment()

        return try {
            val response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
            val clientRequestFinish = now()
            clientRequestLatency.record(clientRequestFinish - clientRequestStart, TimeUnit.MILLISECONDS)
            outgoingFinishedRequestsCounter.increment()

            try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] Failed to parse response for txId: $transactionId", e)
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }
        } catch (e: Exception) {
            val clientRequestFinish = now()
            clientRequestLatency.record(clientRequestFinish - clientRequestStart, TimeUnit.MILLISECONDS)
            outgoingFinishedRequestsCounter.increment()
            logger.error("[$accountName] Payment request failed for txId: $transactionId, payment: $paymentId", e)
            null
        }
    }

    private fun remainingMillis(deadline: Long): Long =
        (deadline - now()).coerceAtLeast(0L)

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun rateLimitPerSec() = properties.rateLimitPerSec
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
