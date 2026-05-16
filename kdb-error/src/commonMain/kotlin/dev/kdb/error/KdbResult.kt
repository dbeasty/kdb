package dev.kdb.error

@Suppress("UNCHECKED_CAST")
public sealed class KdbResult<out T> {
    public data class Success<T>(val value: T) : KdbResult<T>()

    public data class Failure(public val exception: KdbException) : KdbResult<Nothing>()

    public val isSuccess: Boolean get() = this is Success

    public val isFailure: Boolean get() = this is Failure

    public fun getOrNull(): T? = (this as? Success)?.value

    public fun exceptionOrNull(): KdbException? = (this as? Failure)?.exception

    public fun getOrThrow(): T = when (this) {
        is Success -> value
        is Failure -> throw exception
    }

    public inline fun <R> map(transform: (T) -> R): KdbResult<R> = when (this) {
        is Success -> Success(transform(value))
        is Failure -> this
    }

    public inline fun onSuccess(action: (T) -> Unit): KdbResult<T> {
        if (this is Success) action(value)
        return this
    }

    public inline fun onFailure(action: (KdbException) -> Unit): KdbResult<T> {
        if (this is Failure) action(exception)
        return this
    }
}

public inline fun <T> kdbRunCatching(block: () -> T): KdbResult<T> =
    try {
        KdbResult.Success(block())
    } catch (e: KdbException) {
        KdbResult.Failure(e)
    }
