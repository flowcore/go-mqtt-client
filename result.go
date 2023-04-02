package mqtt

type Result[T any] interface {
	Value() T
	Error() error
}

type result[T any] struct {
	value T
	err   error
}

func NewResult[T any](value T, err error) Result[T] {
	return &result[T]{
		value: value,
		err:   err,
	}
}

func (r result[T]) Value() T {
	return r.value
}

func (r result[T]) Error() error {
	return r.err
}
