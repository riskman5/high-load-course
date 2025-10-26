package ru.quipy.payments.logic

class ShouldRetryAfterException(val retryAfter: Long = 15)
    : Exception("Retry after $retryAfter seconds.")