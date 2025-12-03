package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
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
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val retryAfterMillis: Long = 1000

    private val client = OkHttpClient.Builder()
        .readTimeout(Duration.ofMillis(1500))
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .dispatcher(
            Dispatcher(Executors.newVirtualThreadPerTaskExecutor()).apply {
                maxRequests = Int.MAX_VALUE
                maxRequestsPerHost = Int.MAX_VALUE
            }
        )
        .connectionPool(ConnectionPool(10000, 5, TimeUnit.MINUTES))
        .build()

    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

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

    private val retryExecutor = Executors.newVirtualThreadPerTaskExecutor()

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Void> {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        incomingRequestsCounter.increment()

        paymentESService.update(paymentId) {
            it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        return makeAsyncRequestWithRetries(paymentId, amount, transactionId, deadline, 0)
    }

    private fun makeAsyncRequestWithRetries(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
        attempt: Int
    ): CompletableFuture<Void> {
        val future = CompletableFuture<Void>()

        if (deadline - now() < requestAverageProcessingTime.toMillis() || attempt >= 3) {
            incomingFinishedRequestsCounter.increment()
            future.complete(null)
            return future
        }

        if (!ongoingWindow.acquire(
                deadline - now() - requestAverageProcessingTime.toMillis(),
                TimeUnit.MILLISECONDS
            )
        ) {
            logger.error("[$accountName] Payment timeout on our side for txId: $transactionId, payment: $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Timeout - ongoingWindow")
            }
            incomingFinishedRequestsCounter.increment()
            future.complete(null)
            return future
        }

        if (!slidingWindowRateLimiter.tickBlocking(
                deadline - now() - requestAverageProcessingTime.toMillis(),
                TimeUnit.MILLISECONDS
            )
        ) {
            logger.error("[$accountName] Payment timeout on our side for txId: $transactionId, payment: $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Timeout - rateLimiter")
            }
            ongoingWindow.release()
            incomingFinishedRequestsCounter.increment()
            future.complete(null)
            return future
        }

        val clientRequestStart = now()
        outgoingRequestsCounter.increment()

        val request = Request.Builder()
            .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .build()

        client.newCall(request).enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                val clientRequestFinish = now()
                clientRequestLatency.record(clientRequestFinish - clientRequestStart, TimeUnit.MILLISECONDS)

                try {
                    response.use {
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] Failed to parse response for txId: $transactionId", e)
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}")

                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }

                        if (body.result) {
                            outgoingFinishedRequestsCounter.increment()
                            incomingFinishedRequestsCounter.increment()
                            future.complete(null)
                        } else if (attempt < 2) {
                            // Используем Virtual Thread для задержки перед ретраем
                            retryExecutor.submit {
                                Thread.sleep(retryAfterMillis)
                                makeAsyncRequestWithRetries(paymentId, amount, transactionId, deadline, attempt + 1)
                                    .whenComplete { _, _ ->
                                        outgoingFinishedRequestsCounter.increment()
                                        incomingFinishedRequestsCounter.increment()
                                        future.complete(null)
                                    }
                            }
                        } else {
                            outgoingFinishedRequestsCounter.increment()
                            incomingFinishedRequestsCounter.increment()
                            future.complete(null)
                        }
                    }
                } finally {
                    ongoingWindow.release()
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                val clientRequestFinish = now()
                clientRequestLatency.record(clientRequestFinish - clientRequestStart, TimeUnit.MILLISECONDS)

                try {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }

                    if (attempt < 2) {
                        retryExecutor.submit {
                            Thread.sleep(retryAfterMillis)
                            makeAsyncRequestWithRetries(paymentId, amount, transactionId, deadline, attempt + 1)
                                .whenComplete { _, _ ->
                                    outgoingFinishedRequestsCounter.increment()
                                    incomingFinishedRequestsCounter.increment()
                                    future.complete(null)
                                }
                        }
                    } else {
                        outgoingFinishedRequestsCounter.increment()
                        incomingFinishedRequestsCounter.increment()
                        future.complete(null)
                    }
                } finally {
                    ongoingWindow.release()
                }
            }
        })

        return future
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun rateLimitPerSec() = properties.rateLimitPerSec
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()