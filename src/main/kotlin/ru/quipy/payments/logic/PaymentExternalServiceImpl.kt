package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
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
        const val HEDGE_DELAY_MS = 200L
        const val MAX_HEDGES = 5

        const val CIRCUIT_BREAKER_WINDOW_SEC = 20
        const val CIRCUIT_BREAKER_FAILURE_THRESHOLD = 30
        const val CIRCUIT_BREAKER_SLOW_CALL_MS = 400L
        const val CIRCUIT_BREAKER_SLOW_CALL_THRESHOLD = 50
        const val CIRCUIT_BREAKER_MIN_CALLS = 5
        const val CIRCUIT_BREAKER_WAIT_IN_OPEN_MS = 1_500L
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

    private val paymentMutexes = ConcurrentHashMap<UUID, Mutex>()

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
        .builder("payment.hedged.requests")
        .tags("account", accountName)
        .register(meterRegistry)

    private val circuitBreakerRejectedCounter: Counter = Counter
        .builder("payment.circuit_breaker.rejected")
        .tags("account", accountName)
        .register(meterRegistry)

    private val circuitBreaker: CircuitBreaker = run {
        val config = CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
            .slidingWindowSize(CIRCUIT_BREAKER_WINDOW_SEC)
            .failureRateThreshold(CIRCUIT_BREAKER_FAILURE_THRESHOLD.toFloat())
            .slowCallDurationThreshold(java.time.Duration.ofMillis(CIRCUIT_BREAKER_SLOW_CALL_MS))
            .slowCallRateThreshold(CIRCUIT_BREAKER_SLOW_CALL_THRESHOLD.toFloat())
            .minimumNumberOfCalls(CIRCUIT_BREAKER_MIN_CALLS)
            .waitDurationInOpenState(java.time.Duration.ofMillis(CIRCUIT_BREAKER_WAIT_IN_OPEN_MS))
            .permittedNumberOfCallsInHalfOpenState(3)
            .build()
        CircuitBreaker.of("payment-$accountName", config).apply {
            eventPublisher.onStateTransition { event ->
                logger.info("[$accountName] Circuit breaker: ${event.stateTransition.fromState} -> ${event.stateTransition.toState}")
            }
        }
    }

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
        val exp = (RETRY_BASE_MS * 2.0.pow((attempt - 1).toDouble())).toLong()
        return min(exp, RETRY_MAX_MS)
    }

    private fun recordCircuitBreakerResult(success: Boolean, durationMs: Long, message: String?) {
        if (success) {
            circuitBreaker.onSuccess(durationMs, TimeUnit.MILLISECONDS)
        } else {
            circuitBreaker.onError(durationMs, TimeUnit.MILLISECONDS, Exception(message))
        }
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

        val result = performRequestWithRetry(paymentId, amount, transactionId, deadline, 1)

        safeUpdate(paymentId) {
            it.logProcessing(result.success, now(), transactionId, reason = result.message)
        }
        incomingFinishedRequestsCounter.increment()
    }

    private suspend fun performRequestWithRetry(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
        attempt: Int
    ): PaymentResult {
        if (remainingMillis(deadline) < MIN_DEADLINE_BUDGET_MS || attempt > MAX_ATTEMPTS) {
            return PaymentResult(false, "Deadline exceeded or max attempts reached")
        }

        if (!slidingWindowRateLimiter.tickSuspend(
                (remainingMillis(deadline) - MIN_DEADLINE_BUDGET_MS).coerceAtLeast(1),
                TimeUnit.MILLISECONDS
            )
        ) {
            return PaymentResult(false, "Rate limit timeout")
        }

        val result = sendRequest(paymentId, amount, transactionId, deadline)

        if (result.success) {
            return result
        }

        if (attempt < MAX_ATTEMPTS && remainingMillis(deadline) > MIN_DEADLINE_BUDGET_MS + retryDelayMs(attempt)) {
            retryCounter.increment()
            delay(retryDelayMs(attempt))
            return performRequestWithRetry(paymentId, amount, transactionId, deadline, attempt + 1)
        }

        return result
    }

    private suspend fun sendRequest(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
    ): PaymentResult {
        if (!circuitBreaker.tryAcquirePermission()) {
            circuitBreakerRejectedCounter.increment()
            logger.warn("[$accountName] Circuit breaker OPEN, rejecting payment $paymentId")
            return PaymentResult(false, "Circuit breaker is OPEN")
        }

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

            val clientRequestStart = now()
            outgoingRequestsCounter.increment()

            val maxPossibleHedges = ((timeoutMs - MIN_DEADLINE_BUDGET_MS) / HEDGE_DELAY_MS)
                .coerceIn(1, MAX_HEDGES.toLong()).toInt()

            return try {
                coroutineScope {
                    val completableResult = CompletableDeferred<PaymentResult>()
                    val deferreds = mutableListOf<Deferred<Unit>>()
                    val futures = mutableListOf<java.util.concurrent.CompletableFuture<HttpResponse<String>>>()

                    for (i in 0 until maxPossibleHedges) {
                        val deferred = async {
                            if (i > 0) {
                                delay(HEDGE_DELAY_MS * i)
                                if (completableResult.isCompleted) return@async
                                hedgeCounter.increment()
                                outgoingRequestsCounter.increment()
                                logger.info("[$accountName] Sending hedge #$i for payment $paymentId, txId: $transactionId")
                            }

                            val reqTimeoutMs = remainingMillis(deadline)
                                .coerceAtMost(REQUEST_TIMEOUT_MS)
                                .coerceAtLeast(1L)
                            val request = buildRequest(uri, transactionId, reqTimeoutMs)
                            val future = client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                            synchronized(futures) { futures.add(future) }

                            try {
                                val response = future.await()
                                val result = parseResponse(response)
                                if (result.success) {
                                    completableResult.complete(result)
                                }
                            } catch (_: Exception) {
                            }
                        }
                        deferreds.add(deferred)
                    }


                    launch {
                        deferreds.forEach { runCatching { it.await() } }
                        completableResult.complete(PaymentResult(false, "All hedge attempts failed"))
                    }

                    val result = completableResult.await()

                    val clientRequestFinish = now()
                    val durationMs = clientRequestFinish - clientRequestStart
                    clientRequestLatency.record(durationMs, TimeUnit.MILLISECONDS)
                    outgoingFinishedRequestsCounter.increment()
                    recordCircuitBreakerResult(result.success, durationMs, result.message)

                    deferreds.forEach { it.cancel() }
                    synchronized(futures) { futures.forEach { it.cancel(true) } }

                    result
                }
            } catch (e: Exception) {
                val clientRequestFinish = now()
                val durationMs = clientRequestFinish - clientRequestStart
                clientRequestLatency.record(durationMs, TimeUnit.MILLISECONDS)
                outgoingFinishedRequestsCounter.increment()
                recordCircuitBreakerResult(false, durationMs, e.message)
                PaymentResult(false, e.message)
            }
        } finally {
            concurrencySemaphore.release()
        }
    }

    private fun buildRequest(uri: String, transactionId: UUID, timeoutMs: Long): HttpRequest {
        return HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .timeout(Duration.ofMillis(timeoutMs))
            .header("x-idempotency-key", transactionId.toString())
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()
    }

    private fun parseResponse(response: HttpResponse<String>): PaymentResult {
        return try {
            val body = mapper.readValue(response.body(), ExternalSysResponse::class.java)
            PaymentResult(body.result, body.message)
        } catch (e: Exception) {
            PaymentResult(false, e.message)
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
