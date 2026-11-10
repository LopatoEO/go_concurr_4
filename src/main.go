package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Есть сервис, которому необходимо получить информацию о нескольких пользователях из внешнего API.

// Для каждого пользователя нужно вызвать функцию FetchUser.
// Запросы могут выполняться параллельно,
// но количество одновременно выполняемых запросов не должно превышать заданный лимит.

type User struct {
	ID   int
	Name string
}

type Fetcher func(ctx context.Context, id int) (User, error)

type WorkerPool struct {
	res       chan User
	i         int
	err       error
	ctx       context.Context
	cancelCtx context.CancelFunc
	sem       chan struct{}
	lock      sync.Mutex
}

func MakeWorkerPool(ctx context.Context, resLen int, concurrency int) WorkerPool {
	reqCtx, cancelRegCtx := context.WithCancel(ctx)
	return WorkerPool{
		res:       make(chan User, 1),
		err:       nil,
		ctx:       reqCtx,
		cancelCtx: cancelRegCtx,
		sem:       make(chan struct{}, concurrency),
	}
}

func (wp *WorkerPool) Run(ids []int, fetcher Fetcher) ([]User, error) {
	res := make([]User, 0, len(ids))
	go func() {
		in := make(chan struct{})
		close(in)
		for _, id := range ids {
			out := make(chan struct{})
			select {
			case <-wp.ctx.Done():
				return
			case wp.sem <- struct{}{}:
				go wp.worker(id, fetcher, in, out)
				in = out
			}
		}
	}()
	for resIdx := 0; resIdx < len(ids); resIdx++ {
		select {
		case <-wp.ctx.Done():
			wp.lock.Lock()
			defer wp.lock.Unlock()
			fmt.Println("error: ", wp.err)
			return nil, wp.err
		case user := <-wp.res:
			fmt.Println("user: ", user)
			res = append(res, user)
		}
	}
	return res, nil
}

func (wp *WorkerPool) worker(id int, fetcher Fetcher, chin chan struct{}, chout chan struct{}) {
	select {
	case <-wp.ctx.Done():
		<-wp.sem
		return
	default:
	}

	user, err := fetcher(wp.ctx, id)
	if err != nil {
		wp.lock.Lock()
		wp.err = err
		wp.lock.Unlock()
		wp.cancelCtx()
	}

	<-wp.sem

	select {
	case <-chin:
	case <-wp.ctx.Done():
		return
	}

	select {
	case wp.res <- user:
	case <-wp.ctx.Done():
		return
	}

	close(chout)
}

func FetchUsers(
	ctx context.Context,
	ids []int,
	concurrency int,
	fetcher Fetcher,
) ([]User, error) {
	reqCtx, cancelRegCtx := context.WithCancel(ctx)
	defer cancelRegCtx()

	wp := MakeWorkerPool(reqCtx, len(ids), concurrency)
	return wp.Run(ids, fetcher)
}

// требования:
// 1. Запросы должны выполняться параллельно.
// 2. Количество одновременно работающих запросов не должно превышать concurrency.
// 3. Результаты должны возвращаться в том же порядке, в котором переданы ID.
// 4. При ошибке одного из запросов необходимо отменить остальные запросы.
// 5. При отмене родительского context функция должна корректно завершаться.
// 6. Не должно быть утечек горутин.
// 7. Если все запросы успешны, вернуть результаты и nil.
// 8. Если произошла ошибка, вернуть её вызывающему коду.

// Пример использования

func main() {

	ids := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// Пример без ошибок API

	users, err := FetchUsers(
		context.Background(),
		ids,
		2,
		func(ctx context.Context, id int) (User, error) {
			time.Sleep(time.Second)

			return User{
				ID:   id,
				Name: fmt.Sprintf("User-%d", id),
			}, nil
		},
	)
	fmt.Printf("users: %v, err: %v", users, err)

	// Пример с ошибками API

	fmt.Println()
	usersErr, errErr := FetchUsers(
		context.Background(),
		ids,
		2,
		func(ctx context.Context, id int) (User, error) {
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return User{}, ctx.Err()
			}

			if rand.Intn(100) < 30 {
				return User{}, fmt.Errorf("failed to fetch user %d", id)
			}

			return User{
				ID:   id,
				Name: fmt.Sprintf("User-%d", id),
			}, nil
		},
	)
	fmt.Printf("users: %v, err: %v", usersErr, errErr)

}
