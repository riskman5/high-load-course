package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.compareTo

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
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val retryAfterMillis: Long = 50

    private val client: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(1500))
        .version(HttpClient.Version.HTTP_2)
        .executor(Executors.newVirtualThreadPerTaskExecutor())
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

    private val updateExecutor = Executors.newVirtualThreadPerTaskExecutor()

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Void> {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        incomingRequestsCounter.increment()

        updateExecutor.submit {
            paymentESService.update(paymentId) {
                it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
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

            updateExecutor.submit {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Timeout - ongoingWindow")
                }
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

            ongoingWindow.release()

            updateExecutor.submit {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Timeout - rateLimiter")
                }
            }

            incomingFinishedRequestsCounter.increment()
            future.complete(null)
            return future
        }

        val clientRequestStart = now()
        outgoingRequestsCounter.increment()

        val uri = "http://$paymentProviderHostPort/external/process" +
                "?serviceName=$serviceName" +
                "&token=$token" +
                "&accountName=$accountName" +
                "&transactionId=$transactionId" +
                "&paymentId=$paymentId" +
                "&amount=$amount"

        val request = HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .timeout(Duration.ofMillis(requestAverageProcessingTime.toMillis()))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .whenComplete { response, throwable ->
                val clientRequestFinish = now()
                clientRequestLatency.record(clientRequestFinish - clientRequestStart, TimeUnit.MILLISECONDS)

                ongoingWindow.release()

                if (throwable != null) {
                    logger.error(
                        "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                        throwable
                    )

                    updateExecutor.submit {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = throwable.message)
                        }
                    }

                    if (attempt < 2) {
                        CompletableFuture.delayedExecutor(retryAfterMillis, TimeUnit.MILLISECONDS)
                            .execute {
                                makeAsyncRequestWithRetries(paymentId, amount, transactionId, deadline, attempt + 1)
                                    .whenComplete { _, _ -> future.complete(null) }
                            }
                    } else {
                        future.complete(null)
                    }
                } else {
                    val body = try {
                        mapper.readValue(response.body(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Failed to parse response for txId: $transactionId", e)
                        updateExecutor.submit {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Parse error: ${e.message}")
                            }
                        }
                        future.complete(null)
                        null
                    }

                    body?.let {
                        logger.info(
                            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${it.result}"
                        )

                    updateExecutor.submit {
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                    }

                        if (it.result) {
                            future.complete(null)
                        } else if (attempt < 2) {
                            CompletableFuture.delayedExecutor(retryAfterMillis, TimeUnit.MILLISECONDS)
                                .execute {
                                    makeAsyncRequestWithRetries(paymentId, amount, transactionId, deadline, attempt + 1)
                                        .whenComplete { _, _ -> future.complete(null) }
                                }
                        } else {
                            future.complete(null)
                        }
                    }
                }

                outgoingFinishedRequestsCounter.increment()
                incomingFinishedRequestsCounter.increment()
            }

        return future
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun rateLimitPerSec() = properties.rateLimitPerSec
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()