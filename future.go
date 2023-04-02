package mqtt

import "sync"

type Future[T any] interface {
	Value() T
	Error() error
	Done() <-chan any
	Complete(value T, err error) Future[T]
}

func NewFuture[T any]() Future[T] {
	return &future[T]{done: make(chan any)}
}

type future[T any] struct {
	once  sync.Once
	done  chan any
	value T
	err   error
}

func (f *future[T]) Value() T {
	return f.value
}

func (f *future[T]) Error() error {
	return f.err
}

func (f *future[T]) Done() <-chan any {
	return f.done
}

func (f *future[T]) Complete(value T, err error) Future[T] {
	f.once.Do(func() {
		f.value = value
		f.err = err
		close(f.done)
	})
	return f
}
