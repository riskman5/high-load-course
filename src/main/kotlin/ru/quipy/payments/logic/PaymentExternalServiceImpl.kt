package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String
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

    private val client = OkHttpClient.Builder()
        .readTimeout(Duration.ofSeconds(30))
        .dispatcher( Dispatcher( Executors.newFixedThreadPool(10000)).apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .connectionPool(ConnectionPool(parallelRequests, 20, TimeUnit.SECONDS))
        .build()

    private val slidingWindowRateLimiter =
        SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): CompletableFuture<Boolean> {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        val future = CompletableFuture<Boolean>()


        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
            ongoingWindow.acquire(deadline - now() - requestAverageProcessingTime.toMillis(),
                TimeUnit.MILLISECONDS)
            slidingWindowRateLimiter.tickBlocking(
                deadline - now() - requestAverageProcessingTime.toMillis(),
                TimeUnit.MILLISECONDS
            )

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            client.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: java.io.IOException) {
                    try {
                        if (e is SocketTimeoutException) {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                            try {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                                }
                            } catch (u: Exception) {
                                logger.error("[$accountName] Error while updating ES on timeout for payment $paymentId, txId: $transactionId", u)
                            }
                        } else {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                            try {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = e.message)
                                }
                            } catch (u: Exception) {
                                logger.error("[$accountName] Error while updating ES on failure for payment $paymentId, txId: $transactionId", u)
                            }
                        }
                    } finally {
                        try {
                            ongoingWindow.release()
                        } catch (u: Exception) {
                            logger.error("[$accountName] Error releasing ongoingWindow", u)
                        }
                        future.complete(false)
                    }
                }

                override fun onResponse(call: Call, response: Response) {
                    try {
                        val bodyText = try {
                            response.body?.string()
                        } catch (_: Exception) {
                            null
                        }

                        val body = try {
                            mapper.readValue(bodyText, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error(
                                "[$accountName] [ERROR] Payment processed for txId: $transactionId, " +
                                        "payment: $paymentId, result code: ${response.code}, reason: $bodyText",
                                e
                            )
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message ?: bodyText)
                        }

                        logger.warn(
                            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                    "succeeded: ${body.result}, message: ${body.message}"
                        )


                        try {
                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }
                        } catch (u: Exception) {
                            logger.error("[$accountName] Error while updating ES on response for payment $paymentId, txId: $transactionId", u)
                        }

                        future.complete(body.result)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Error processing response for txId: $transactionId, payment: $paymentId", e)
                        try {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        } catch (u: Exception) {
                            logger.error("[$accountName] Error while updating ES in exception handler for payment $paymentId, txId: $transactionId", u)
                        }
                        future.complete(false)
                    } finally {
                        try {
                            ongoingWindow.release()
                        } catch (u: Exception) {
                            logger.error("[$accountName] Error releasing ongoingWindow", u)
                        }
                    }
                }
            })
        } catch (e: Exception) {
            if (e is SocketTimeoutException) {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
            } else {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }
            ongoingWindow.release()
            future.complete(false)
        }

        return future
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun rateLimitPerSec(): Int = properties.rateLimitPerSec

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()