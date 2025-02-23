package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var rdb *redis.ClusterClient

func AcquireLock(ctx context.Context, lockKey, lockValue string, ttl time.Duration) (bool, error) {
	// Lua 脚本：如果锁不存在，则设置锁并设置过期时间
	script := `
        if redis.call("SETNX", KEYS[1], ARGV[1]) == 1 then
            return redis.call("EXPIRE", KEYS[1], ARGV[2])
        else
            return 0
        end
    `

	// 执行 Lua 脚本
	result, err := rdb.Eval(ctx, script, []string{lockKey}, lockValue, int(ttl.Seconds())).Int()
	if err != nil {
		return false, fmt.Errorf("failed to run script: %w", err)
	}

	// 如果返回值为 1，表示锁获取成功
	if result == 1 {
		return true, nil
	}
	return false, nil
}

// ReleaseLock 尝试释放分布式锁
// - lockKey: 锁的键名
// - lockValue: 锁的唯一标识符
func ReleaseLock(ctx context.Context, lockKey, lockValue string) (bool, error) {
	// Lua 脚本：只有当锁的值与当前值匹配时，才释放锁
	script := `
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
    `

	// 执行 Lua 脚本
	result, err := rdb.Eval(ctx, script, []string{lockKey}, lockValue).Int()
	if err != nil {
		return false, fmt.Errorf("failed to run script: %w", err)
	}

	// 如果返回值为 1，表示锁释放成功
	if result == 1 {
		return true, nil
	}
	return false, nil
}
