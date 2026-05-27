package error

// Result is a success/failure sum type.
type Result[T any] struct {
	value T
	err   Exception
	ok    bool
}

func Ok[T any](v T) Result[T] { return Result[T]{value: v, ok: true} }

func Fail[T any](e Exception) Result[T] { return Result[T]{err: e, ok: false} }

func (r Result[T]) IsSuccess() bool { return r.ok }
func (r Result[T]) IsFailure() bool { return !r.ok }

func (r Result[T]) Value() (T, bool) {
	if r.ok {
		return r.value, true
	}
	var zero T
	return zero, false
}

func (r Result[T]) Exception() Exception {
	if !r.ok {
		return r.err
	}
	return nil
}

func (r Result[T]) MustValue() T {
	if !r.ok {
		panic(r.err)
	}
	return r.value
}

// Run catches Exception values into Result.
func Run[T any](fn func() (T, error)) Result[T] {
	v, err := fn()
	if err == nil {
		return Ok(v)
	}
	if e, ok := err.(Exception); ok {
		return Fail[T](e)
	}
	return Fail[T](NewDecodeError(err.Error(), -1, err))
}
