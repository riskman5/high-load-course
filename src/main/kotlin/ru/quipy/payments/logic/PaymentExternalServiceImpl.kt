package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import io.micrometer.core.instrument.Timer
import okhttp3.ConnectionPool
import okhttp3.Protocol
import java.io.IOException
import java.util.concurrent.CompletableFuture


// Advice: always treat time as a Duration
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
        .callTimeout(Duration.ofMillis(15000))
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .dispatcher(
            okhttp3.Dispatcher().apply {
                maxRequests = 10000
                maxRequestsPerHost = 10000
            }
        )
        .connectionPool(ConnectionPool(10000, 5, TimeUnit.MINUTES))
        .build()
    private val slidingWindowRateLimiter =
        SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)
    private val incomingRequestsCounter: Counter = Counter
        .builder("incoming.requests")
        .description("Количество завершенных входящих запросов")
        .tags("account", properties.accountName)
        .register(meterRegistry)
    private val incomingFinishedRequestsCounter: Counter = Counter
        .builder("incoming.finished.requests")
        .description("Количество завершенных входящих запросов")
        .tags("account", properties.accountName)
        .register(meterRegistry)
    private val outgoingRequestsCounter: Counter = Counter
        .builder("outgoing.requests")
        .description("Количество исходящих запросов")
        .tags("account", properties.accountName)
        .register(meterRegistry)
    private val outgoingFinishedRequestsCounter: Counter = Counter
        .builder("outgoing.finished.requests")
        .description("Количество завершенных исходящих запросов")
        .tags("account", properties.accountName)
        .register(meterRegistry)
    private val clientReqeustLatency: Timer =
        Timer.builder("client.request.latency")
            .description("Request latency.")
            .publishPercentiles(0.5, 0.8, 0.9)
            .register(meterRegistry)


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): CompletableFuture<Void> {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        incomingRequestsCounter.increment()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
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

        if (deadline - System.currentTimeMillis() < requestAverageProcessingTime.toMillis() || attempt >= 3) {
            incomingFinishedRequestsCounter.increment()
            future.complete(null)
            return future
        }

        ongoingWindow.acquire(deadline - now() - requestAverageProcessingTime.toMillis(),
            TimeUnit.MILLISECONDS)
        slidingWindowRateLimiter.tickBlocking(
            deadline - now() - requestAverageProcessingTime.toMillis(),
            TimeUnit.MILLISECONDS
        )

        outgoingRequestsCounter.increment()

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        client.newCall(request).enqueue(object : okhttp3.Callback {
            override fun onResponse(call: okhttp3.Call, response: okhttp3.Response) {
                try {
                    response.use {
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        if (body.result) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(true, now(), transactionId, reason = body.message)
                            }
                            future.complete(null)
                        } else if (attempt < 2) {
                            CompletableFuture.delayedExecutor(retryAfterMillis, TimeUnit.MILLISECONDS).execute {
                                makeAsyncRequestWithRetries(paymentId, amount, transactionId, deadline, attempt + 1)
                                    .whenComplete { _, _ -> future.complete(null) }
                            }
                        } else {
                            future.complete(null)
                        }
                    }
                } finally {
                    ongoingWindow.release()
                }
            }

            override fun onFailure(call: okhttp3.Call, e: IOException) {
                try {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                    if (attempt < 2) {
                        makeAsyncRequestWithRetries(paymentId, amount, transactionId, deadline, attempt + 1)
                            .whenComplete { _, _ -> future.complete(null) }
                    } else {
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

    override fun rateLimitPerSec(): Int {
        return properties.rateLimitPerSec
    }

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()