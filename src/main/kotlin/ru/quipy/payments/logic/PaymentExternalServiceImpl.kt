package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
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
        .build()

    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

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

        makeRequestWithRetries(paymentId, amount, transactionId, deadline)
    }

    private suspend fun makeRequestWithRetries(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ) {
        for (attempt in 0 until 3) {
            if (deadline - now() < requestAverageProcessingTime.toMillis()) {
                incomingFinishedRequestsCounter.increment()
                return
            }

            if (!ongoingWindow.acquireSuspend(
                    deadline - now() - requestAverageProcessingTime.toMillis(),
                    TimeUnit.MILLISECONDS
                )
            ) {
                logger.error("[$accountName] Payment timeout on our side for txId: $transactionId, payment: $paymentId")

                updateScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Timeout - ongoingWindow")
                    }
                }

                incomingFinishedRequestsCounter.increment()
                return
            }

            if (!slidingWindowRateLimiter.tickSuspend(
                    deadline - now() - requestAverageProcessingTime.toMillis(),
                    TimeUnit.MILLISECONDS
                )
            ) {
                logger.error("[$accountName] Payment timeout on our side for txId: $transactionId, payment: $paymentId")

                ongoingWindow.release()

                updateScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Timeout - rateLimiter")
                    }
                }

                incomingFinishedRequestsCounter.increment()
                return
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

            try {
                val response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()

                val clientRequestFinish = now()
                clientRequestLatency.record(clientRequestFinish - clientRequestStart, TimeUnit.MILLISECONDS)
                ongoingWindow.release()

                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] Failed to parse response for txId: $transactionId", e)
                    updateScope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Parse error: ${e.message}")
                        }
                    }
                    outgoingFinishedRequestsCounter.increment()
                    incomingFinishedRequestsCounter.increment()
                    return
                }

                logger.info(
                    "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}"
                )

                updateScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }

                outgoingFinishedRequestsCounter.increment()
                incomingFinishedRequestsCounter.increment()

                if (body.result) {
                    return
                }

                if (attempt < 2) {
                    delay(retryAfterMillis)
                    continue
                }
                return

            } catch (throwable: Exception) {
                val clientRequestFinish = now()
                clientRequestLatency.record(clientRequestFinish - clientRequestStart, TimeUnit.MILLISECONDS)
                ongoingWindow.release()

                logger.error(
                    "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                    throwable
                )

                updateScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = throwable.message)
                    }
                }

                outgoingFinishedRequestsCounter.increment()
                incomingFinishedRequestsCounter.increment()

                if (attempt < 2) {
                    delay(retryAfterMillis)
                    continue
                }
                return
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun rateLimitPerSec() = properties.rateLimitPerSec
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
