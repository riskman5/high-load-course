package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import kotlin.math.min
import kotlin.math.pow

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
        const val MAX_ATTEMPTS = 4
        const val RETRY_BASE_MS = 30L
        const val RETRY_MAX_MS = 500L
        const val MIN_DEADLINE_BUDGET_MS = 50L
        const val REQUEST_TIMEOUT_MS = 2000L
        const val MIN_ADAPTIVE_TIMEOUT_MS = 10L
        const val HEDGE_DELAY_MIN_MS = 50L
        const val HEDGE_DELAY_MAX_MS = 800L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val concurrencySemaphore = Semaphore(parallelRequests)

    private val client: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(2000))
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        (rateLimitPerSec * 0.98).toLong().coerceAtLeast(1),
        Duration.ofSeconds(1)
    )

    private val dbExecutor = Executors.newFixedThreadPool(16)
    private val updateScope = CoroutineScope(dbExecutor.asCoroutineDispatcher())
    private val hedgeScope = CoroutineScope(Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher())

    private val paymentMutexes = ConcurrentHashMap<UUID, Mutex>()

    @Volatile
    private var emaLatency = properties.averageProcessingTime.toMillis().coerceAtLeast(MIN_ADAPTIVE_TIMEOUT_MS)

    private val hedgeDelayMs: Long
        get() = (emaLatency * 1.5).toLong().coerceIn(HEDGE_DELAY_MIN_MS, HEDGE_DELAY_MAX_MS)

    private fun updateLatency(durationMs: Long) {
        val alpha = 0.2
        emaLatency = (alpha * durationMs + (1 - alpha) * emaLatency).toLong()
    }

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

    private val retryCounter: Counter = Counter
        .builder("payment.retries")
        .tags("account", accountName)
        .register(meterRegistry)

    private val hedgeCounter: Counter = Counter
        .builder("payment.hedges")
        .tags("account", accountName)
        .register(meterRegistry)

    private fun safeUpdate(paymentId: UUID, block: (PaymentAggregateState) -> ru.quipy.domain.Event<PaymentAggregate>) {
        updateScope.launch {
            val mutex = paymentMutexes.getOrPut(paymentId) { Mutex() }
            mutex.withLock {
                while (true) {
                    try {
                        paymentESService.update(paymentId) { block(it) }
                        break
                    } catch (_: IllegalArgumentException) {
                        delay(10)
                    } catch (e: Exception) {
                        logger.error("[$accountName] DB update failed for payment: $paymentId", e)
                        break
                    }
                }
            }
            paymentMutexes.remove(paymentId)
        }
    }

    private fun retryDelayMs(attempt: Int): Long {
        val expBackoff = (RETRY_BASE_MS * 2.0.pow((attempt - 1).toDouble())).toLong()
        val jitter = ThreadLocalRandom.current().nextLong(0, 15)
        return min(expBackoff + jitter, RETRY_MAX_MS)
    }

    override suspend fun performPayment(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        if (remainingMillis(deadline) < MIN_DEADLINE_BUDGET_MS) {
            return
        }

        val transactionId = UUID.randomUUID()
        incomingRequestsCounter.increment()

        safeUpdate(paymentId) {
            it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val result = performRequestWithHedgingAndRetry(paymentId, amount, transactionId, deadline)

        safeUpdate(paymentId) {
            it.logProcessing(result.success, now(), transactionId, reason = result.message)
        }
        incomingFinishedRequestsCounter.increment()
    }

    private suspend fun performRequestWithHedgingAndRetry(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
    ): PaymentResult {
        for (attempt in 1..MAX_ATTEMPTS) {
            if (remainingMillis(deadline) < MIN_DEADLINE_BUDGET_MS) {
                return PaymentResult(false, "Deadline exceeded")
            }

            if (!slidingWindowRateLimiter.tickSuspend(
                    (remainingMillis(deadline) - MIN_DEADLINE_BUDGET_MS).coerceAtLeast(1),
                    TimeUnit.MILLISECONDS
                )
            ) {
                return PaymentResult(false, "Rate limit timeout")
            }

            val result = performHedgedRequest(paymentId, amount, transactionId, deadline)

            if (result.success) {
                return result
            }

            if (attempt < MAX_ATTEMPTS && remainingMillis(deadline) > MIN_DEADLINE_BUDGET_MS + retryDelayMs(attempt)) {
                retryCounter.increment()
                delay(retryDelayMs(attempt))
            } else {
                return result
            }
        }
        return PaymentResult(false, "Max attempts reached")
    }

    private suspend fun performHedgedRequest(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
    ): PaymentResult {
        val deferred = CompletableDeferred<PaymentResult>()

        val primaryJob = hedgeScope.launch {
            val res = sendRequest(paymentId, amount, transactionId, deadline)
            deferred.complete(res)
        }

        val hedgeJob = hedgeScope.launch {
            delay(hedgeDelayMs)
            if (!deferred.isCompleted && remainingMillis(deadline) > MIN_DEADLINE_BUDGET_MS) {
                hedgeCounter.increment()
                if (slidingWindowRateLimiter.tick()) {
                    val res = sendRequest(paymentId, amount, transactionId, deadline)
                    deferred.complete(res)
                }
            }
        }

        val timeoutMs = (remainingMillis(deadline) - MIN_DEADLINE_BUDGET_MS).coerceAtLeast(1L)
        val result = withTimeoutOrNull(timeoutMs) { deferred.await() }
            ?: PaymentResult(false, "Deadline timeout waiting for hedged response")

        primaryJob.cancel()
        hedgeJob.cancel()

        return result
    }

    private suspend fun sendRequest(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
    ): PaymentResult {
        concurrencySemaphore.acquire()
        try {
            val timeoutMs = remainingMillis(deadline)
                .coerceAtMost(REQUEST_TIMEOUT_MS)
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
                .header("x-idempotency-key", transactionId.toString())
                .timeout(Duration.ofMillis(timeoutMs))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()

            val clientRequestStart = now()
            outgoingRequestsCounter.increment()

            return try {
                val response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
                val latency = now() - clientRequestStart
                updateLatency(latency)
                clientRequestLatency.record(latency, TimeUnit.MILLISECONDS)
                outgoingFinishedRequestsCounter.increment()

                try {
                    val body = mapper.readValue(response.body(), ExternalSysResponse::class.java)
                    PaymentResult(body.result, body.message)
                } catch (e: Exception) {
                    PaymentResult(false, e.message)
                }
            } catch (e: Exception) {
                val latency = now() - clientRequestStart
                updateLatency(latency)
                clientRequestLatency.record(latency, TimeUnit.MILLISECONDS)
                outgoingFinishedRequestsCounter.increment()
                PaymentResult(false, e.message)
            }
        } finally {
            concurrencySemaphore.release()
        }
    }

    private fun remainingMillis(deadline: Long): Long =
        (deadline - now()).coerceAtLeast(0L)

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun rateLimitPerSec() = properties.rateLimitPerSec
    override fun name() = properties.accountName

    private data class PaymentResult(val success: Boolean, val message: String?)
}

fun now() = System.currentTimeMillis()
