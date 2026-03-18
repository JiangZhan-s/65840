package lock

import (
	"time"

	rpc "6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk 是一个 go 接口，表示键值对 clerk：该接口隐藏了 ck 的具体 Clerk 类型，但保证 ck 支持 Put 和 Get 方法。
	// 测试器在调用 MakeLock() 时会传入 clerk。
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
	clientID string
}

// 测试器会调用 MakeLock() 并传入一个 k/v clerk；你的代码可以通过调用 lk.ck.Put() 或 lk.ck.Get() 来执行 Put 或 Get 操作。
//
// 该接口通过 lockname 参数支持多个锁；不同名字的锁应该相互独立。
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockname: lockname,
		clientID: kvtest.RandValue(8),
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, getErr := lk.ck.Get(lk.lockname)

		if getErr == rpc.OK && val == lk.clientID {
			return
		}

		if getErr == rpc.ErrNoKey || val == "" {
			lk.ck.Put(lk.lockname, lk.clientID, ver)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.lockname)

		if err == rpc.ErrNoKey || val == "" || val != lk.clientID {
			return
		}

		if putErr := lk.ck.Put(lk.lockname, "", ver); putErr == rpc.OK {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}
}
