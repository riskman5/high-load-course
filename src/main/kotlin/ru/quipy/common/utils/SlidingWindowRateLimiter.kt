package ru.quipy.common.utils

import kotlinx.coroutines.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }

    private val windowMs = window.toMillis()
    private val timestamps = ArrayDeque<Long>()
    private val lock = ReentrantLock()

    override fun tick(): Boolean {
        if (rate <= 0L) return false
        return lock.withLock {
            val now = System.currentTimeMillis()
            cleanup(now)
            if (timestamps.size < rate.toInt()) {
                timestamps.addLast(now)
                true
            } else {
                false
            }
        }
    }

    fun tickBlocking(timeout: Long, unit: TimeUnit): Boolean {
        if (timeout <= 0) return false
        val deadline = System.currentTimeMillis() + unit.toMillis(timeout)
        while (System.currentTimeMillis() < deadline) {
            if (tick()) return true
            Thread.sleep(1)
        }
        return false
    }

    suspend fun tickSuspend(timeout: Long, unit: TimeUnit): Boolean {
        if (timeout <= 0) return false
        val deadline = System.currentTimeMillis() + unit.toMillis(timeout)
        while (System.currentTimeMillis() < deadline) {
            if (tick()) return true
            delay(1)
        }
        return false
    }

    private fun cleanup(now: Long) {
        val windowStart = now - windowMs
        while (timestamps.isNotEmpty() && timestamps.first() <= windowStart) {
            timestamps.removeFirst()
        }
    }
}