/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workqueue

import (
	"math"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// TODO: 限速周期, 一个限速周期是指从执行 AddRateLimited 方法到执行完 Forget 方法之间的时间.
// 如果该元素被 Forget 方法处理完, 则清空排队数.
type RateLimiter interface {
	// When gets an item and gets to decide how long that item should wait
	// When 获取指定元素应该等待的时间
	When(item interface{}) time.Duration
	// Forget indicates that an item is finished being retried.  Doesn't matter whether its for perm failing
	// or for success, we'll stop tracking it
	// Forget 释放指定元素, 清空该元素的排队数
	Forget(item interface{})
	// NumRequeues returns back how many failures the item has had
	// NumRequeues 获取指定元素的排队数
	NumRequeues(item interface{}) int
}

// DefaultControllerRateLimiter is a no-arg constructor for a default rate limiter for a workqueue.  It has
// both overall and per-item rate limiting.  The overall is a token bucket and the per-item is exponential
// DefaultControllerRateLimiter 实例化一个使用混合模式的限速器, 这里同时使用令牌桶算法和排队指数算法.
func DefaultControllerRateLimiter() RateLimiter {
	return NewMaxOfRateLimiter(
		NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
		// 10 qps, 100 bucket size.  This is only for retry speed and its only the overall factor (not per item)
		&BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)
}

// BucketRateLimiter adapts a standard bucket to the workqueue ratelimiter API
// BucketRateLimiter 令牌桶算法. 令牌桶算法是通过 Go 语言的第三方库 golang.org/x/time/rate 实现的.令牌桶算法
// 内部实现了一个存放 token(令牌)的 "桶", 初始时 "桶" 是空的, token 会以固定速率往 "桶" 里填充, 直到将其填满
// 为止, 多余的 token 会被丢弃. 每个元素都会从令牌桶得到一个 token, 只有得到 token 的元素才允许通过(accept),
// 而没有得到 token 的元素处于等待状态. 令牌桶算法通过控制发放 token 来达到限速目的.
type BucketRateLimiter struct {
	*rate.Limiter // 令牌桶
}

var _ RateLimiter = &BucketRateLimiter{}

// When 获取指定元素应该等待的时间
func (r *BucketRateLimiter) When(item interface{}) time.Duration {
	return r.Limiter.Reserve().Delay()
}

// NumRequeues 获取指定元素的排队数, 令牌桶算法的排队数为 0
func (r *BucketRateLimiter) NumRequeues(item interface{}) int {
	return 0
}

func (r *BucketRateLimiter) Forget(item interface{}) {
}

// ItemExponentialFailureRateLimiter does a simple baseDelay*2^<num-failures> limit
// dealing with max failures and expiration are up to the caller
// ItemExponentialFailureRateLimiter 排队指数算法.
// 排队指数算法将相同元素的排队数作为指数, 排队数增大, 速率限制呈指数级增长, 但其最大值不会超过 maxDelay.
// 元素的排队数统计是有限速周期的, 一个限速周期是指从执行 AddRateLimited 方法到执行完 Forget 方法之间的
// 时间. 如果该元素被 Forget 方法处理完, 则清空排队数.
type ItemExponentialFailureRateLimiter struct {
	failuresLock sync.Mutex
	// 该字段用于统计元素排队数, 每当 AddRateLimited 方法插入新元素时, 会为该字段加 1
	failures map[interface{}]int

	// 限速队列利用延迟队列的特性, 延迟多个相同元素的插入时间, 达到限速目的.
	// TODO：在同一个限速周期内, 如果不存在相同元素, 那么所有元素的延迟时间为 baseDelay; 而在一个限速周期内,
	// 如果存在相同元素, 那么相同元素的延迟时间呈指数级增长, 最长延迟时间不超过 maxDelay.
	baseDelay time.Duration // 最初的限速单位(默认为 5ms)
	maxDelay  time.Duration // 最大限速单位(默认 1000s)
}

var _ RateLimiter = &ItemExponentialFailureRateLimiter{}

func NewItemExponentialFailureRateLimiter(baseDelay time.Duration, maxDelay time.Duration) RateLimiter {
	return &ItemExponentialFailureRateLimiter{
		failures:  map[interface{}]int{},
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
	}
}

func DefaultItemBasedRateLimiter() RateLimiter {
	return NewItemExponentialFailureRateLimiter(time.Millisecond, 1000*time.Second)
}

// When 获取指定元素应该等待的时间
func (r *ItemExponentialFailureRateLimiter) When(item interface{}) time.Duration {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	// 排队指数算法核心实现如下.
	// 假定 baseDelay 是 1ms, maxDelay 是 1000s. 假设在一个限速周期内通过 AddRateLimited 方法插入 10
	// 个相同元素, 那么第 1 个元素会通过延迟队列的 AddAfter 方法插入并设置延迟时间为 1ms(即 baseDelay),
	// 第 2 个相同元素的延迟时间为 2ms, 第 3 个相同元素的延迟时间为 4ms, 第 4 个相同元素的延迟时间为 8ms ...
	// 第 10 个相同元素的延迟时间为 512ms, 最长延迟时间不超过 1000s(即 maxDelay).

	exp := r.failures[item]
	r.failures[item] = r.failures[item] + 1

	// The backoff is capped such that 'calculated' value never overflows.
	backoff := float64(r.baseDelay.Nanoseconds()) * math.Pow(2, float64(exp))
	if backoff > math.MaxInt64 {
		return r.maxDelay
	}

	calculated := time.Duration(backoff)
	if calculated > r.maxDelay {
		return r.maxDelay
	}

	return calculated
}

// NumRequeues 获取指定元素的排队数
func (r *ItemExponentialFailureRateLimiter) NumRequeues(item interface{}) int {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	return r.failures[item]
}

// Forget 释放指定元素, 即清空该元素的排队数
func (r *ItemExponentialFailureRateLimiter) Forget(item interface{}) {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	delete(r.failures, item)
}

// ItemFastSlowRateLimiter does a quick retry for a certain number of attempts, then a slow retry after that
// ItemFastSlowRateLimiter 计数器算法
// 计数器算法原理是: 限制一段时间内允许通过的元素数量, 如在 1分钟内只允许通过 100 个元素, 每插入一个元素, 计数器自增 1,
// 当计数器数到 100 的阈值且还在限速周期内时, 则不允许元素再通过. 但 WorkQueue 在此基础上扩展了 fast 和 slow 速率.
type ItemFastSlowRateLimiter struct {
	failuresLock sync.Mutex
	// 用于统计元素排队数, 每当 AddRateLimited 方法插入新元素时, 会为该字段加 1
	failures map[interface{}]int

	// 用于控制从 fast 速率转换到 slow 速率.
	maxFastAttempts int
	// 如下两个字段分别用于定义 fast、slow 速率的
	fastDelay time.Duration
	slowDelay time.Duration
}

var _ RateLimiter = &ItemFastSlowRateLimiter{}

func NewItemFastSlowRateLimiter(fastDelay, slowDelay time.Duration, maxFastAttempts int) RateLimiter {
	return &ItemFastSlowRateLimiter{
		failures:        map[interface{}]int{},
		fastDelay:       fastDelay,
		slowDelay:       slowDelay,
		maxFastAttempts: maxFastAttempts,
	}
}

// When 获取指定元素应该等待的时间
func (r *ItemFastSlowRateLimiter) When(item interface{}) time.Duration {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	// 计数器算法核心实现如下.
	// 假设 fastDelay 是 5ms, slowDelay 是 10s, maxFastAttempts 是 3. 在一个限速周期内通过
	// AddRateLimited 方法插入 4 个相同的元素, 那么前 3 个元素使用 fastDelay 定义的 fast 速率,
	// 当触发 maxFastAttempts 字段时, 第 4 个元素使用 slowDelay 定义的 slow 速率.

	r.failures[item] = r.failures[item] + 1

	if r.failures[item] <= r.maxFastAttempts {
		return r.fastDelay
	}

	return r.slowDelay
}

// NumRequeues 获取指定元素的排队数
func (r *ItemFastSlowRateLimiter) NumRequeues(item interface{}) int {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	return r.failures[item]
}

// Forget 释放指定元素, 清空该元素的排队数
func (r *ItemFastSlowRateLimiter) Forget(item interface{}) {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	delete(r.failures, item)
}

// MaxOfRateLimiter calls every RateLimiter and returns the worst case response
// When used with a token bucket limiter, the burst could be apparently exceeded in cases where particular items
// were separately delayed a longer time.
// MaxOfRateLimiter 混合模式, 将多种限速算法混合使用, 即多种限速算法同时生效.
type MaxOfRateLimiter struct {
	limiters []RateLimiter
}

// When 获取指定元素应该等待的时间
func (r *MaxOfRateLimiter) When(item interface{}) time.Duration {
	ret := time.Duration(0)
	// 混合模式下返回元素的应等待时间是各模式下的最大值
	for _, limiter := range r.limiters {
		curr := limiter.When(item)
		if curr > ret {
			ret = curr
		}
	}

	return ret
}

func NewMaxOfRateLimiter(limiters ...RateLimiter) RateLimiter {
	return &MaxOfRateLimiter{limiters: limiters}
}

// NumRequeues 返回指定元素的排队数
func (r *MaxOfRateLimiter) NumRequeues(item interface{}) int {
	ret := 0
	for _, limiter := range r.limiters {
		curr := limiter.NumRequeues(item)
		if curr > ret {
			ret = curr
		}
	}

	return ret
}

// Forget 释放该元素, 清空该元素的排队数
func (r *MaxOfRateLimiter) Forget(item interface{}) {
	for _, limiter := range r.limiters {
		limiter.Forget(item)
	}
}
