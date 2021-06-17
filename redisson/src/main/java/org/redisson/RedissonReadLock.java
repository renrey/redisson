/**
 * Copyright (c) 2013-2021 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * Lock will be removed automatically if client disconnects.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonReadLock extends RedissonLock implements RLock {

    public RedissonReadLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getRawName());
    }
    
    String getWriteLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }

    String getReadWriteTimeoutNamePrefix(long threadId) {
        return suffixName(getRawName(), getLockName(threadId)) + ":rwlock_timeout";
    }
    
    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        /**
         * keys[2]: {key}:UUID:threadId:rwlock_timeout
         */
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                                // 没有mode，等于没有锁
                                "if (mode == false) then " +
                                  // 加入read的mode
                                  "redis.call('hset', KEYS[1], 'mode', 'read'); " +
								  // 初始化线程标识、计数器
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                  // set {key}:UUID:threadId:rwlock_timeout:1 1
                                  "redis.call('set', KEYS[2] .. ':1', 1); " +
                                  // 设置过期时间 {key}:UUID:threadId:rwlock_timeout:1
                                  "redis.call('pexpire', KEYS[2] .. ':1', ARGV[1]); " +
                                  // 设置key过期时间
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                  "return nil; " +
                                "end; " +

                                // 已有读锁 or 已有当前线程的写锁
                                "if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then " +
                                  // 计数器+1
                                  "local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
								  // {key}:UUID:threadId:rwlock_timeout:当前计数
                                  "local key = KEYS[2] .. ':' .. ind;" +

                                  // set {key}:UUID:threadId:rwlock_timeout:当前计数 1
                                  "redis.call('set', key, 1); " +
								  // 设置过期时间，{key}:UUID:threadId:rwlock_timeout:当前计数
                                  "redis.call('pexpire', key, ARGV[1]); " +
								  // 剩余时间
                                  "local remainTime = redis.call('pttl', KEYS[1]); " +
                                  // 延长key过期时间，剩余时间or默认间隔中的max
                                  "redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1])); " +
                                  "return nil; " +
                                "end;" +
                                "return redis.call('pttl', KEYS[1]);",
                        Arrays.<Object>asList(getRawName(), getReadWriteTimeoutNamePrefix(threadId)),
                        unit.toMillis(leaseTime), getLockName(threadId), getWriteLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);

        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                // 无锁
                "if (mode == false) then " +
                     // 发布解锁消息到 redisson_rwlock_key
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                 // 对象是否存在当前线程标识
                "local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); " +
				// 不是当前线程持有锁
                "if (lockExists == 0) then " +
                    "return nil;" +
                "end; " +
                // 当前线程持有锁
                // 计数器-1
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " +
				// 计数器=0， 删除线程标识hdel
                "if (counter == 0) then " +
                    "redis.call('hdel', KEYS[1], ARGV[2]); " + 
                "end;" +


                // 删除对象：{key}:UUID:threadId:rwlock_timeout:当前计数
                "redis.call('del', KEYS[3] .. ':' .. (counter+1)); " +

                // hlen > 1, 即还有其他线程占有读锁or当前线程持有写锁，除了mode，还有其他key
                "if (redis.call('hlen', KEYS[1]) > 1) then " +
                    "local maxRemainTime = -3; " +
                        /**
                         * 其实就是获取所有占有线程中，剩余最大的存活时间,用于给锁延长时间
                          */
                    // 获取锁的所有key，并循环
                    "local keys = redis.call('hkeys', KEYS[1]); " + 
                    "for n, key in ipairs(keys) do " +
                        // 获取key的值
                        "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                        // 如果值是数字，就是计数器，不是代表mode直接跳过
                        "if type(counter) == 'number' then " +
                            // 遍历已有计数值
                            "for i=counter, 1, -1 do " + 
                                // 获取每个{key}:UUID:threadId:rwlock_timeout:当前计数 的剩余存活时间
                                "local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); " +
                                // 获取最大的剩余存活时长
                                "maxRemainTime = math.max(remainTime, maxRemainTime);" +
                            "end; " + 
                        "end; " + 
                    "end; " +

                     // 使用maxRemainTime为锁key延长时间，然后返回
                    "if maxRemainTime > 0 then " +
                        "redis.call('pexpire', KEYS[1], maxRemainTime); " +
                        "return 0; " +
                    "end;" + 
                    // 没有读锁或者全部已过期，还剩下写锁，直接返回
                    "if mode == 'write' then " + 
                        "return 0;" + 
                    "end; " +
                "end; " +
                // 无线程占有or 已占有线程都过期了
                // 删除锁
                "redis.call('del', KEYS[1]); " +
				// 发布解锁消息， redisson_rwlock:key
                "redis.call('publish', KEYS[2], ARGV[1]); " +
                "return 1; ",
                Arrays.<Object>asList(getRawName(), getChannelName(), timeoutPrefix, keyPrefix),
                LockPubSub.UNLOCK_MESSAGE, getLockName(threadId));
    }

    protected String getKeyPrefix(long threadId, String timeoutPrefix) {
        return timeoutPrefix.split(":" + getLockName(threadId))[0];
    }
    
    @Override
    protected RFuture<Boolean> renewExpirationAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);
        
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local counter = redis.call('hget', KEYS[1], ARGV[2]); " +
                "if (counter ~= false) then " +
                    // 延长锁存活时间
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    // 延长每个{key}:UUID:threadId:rwlock_timeout:当前计数的存活时间
                    "if (redis.call('hlen', KEYS[1]) > 1) then " +
                        "local keys = redis.call('hkeys', KEYS[1]); " + 
                        "for n, key in ipairs(keys) do " + 
                            "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                            "if type(counter) == 'number' then " + 
                                "for i=counter, 1, -1 do " +
                                    // 延长时间为间隔时间，{key}:UUID:threadId:rwlock_timeout:当前计数
                                    "redis.call('pexpire', KEYS[2] .. ':' .. key .. ':rwlock_timeout:' .. i, ARGV[1]); " + 
                                "end; " + 
                            "end; " + 
                        "end; " +
                    "end; " +
                    
                    "return 1; " +
                "end; " +
                "return 0;",
            Arrays.<Object>asList(getRawName(), keyPrefix),
            internalLockLeaseTime, getLockName(threadId));
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hget', KEYS[1], 'mode') == 'read') then " +
                    "redis.call('del', KEYS[1]); " +
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "return 0; ",
                Arrays.<Object>asList(getRawName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.HGET, getRawName(), "mode");
        String res = get(future);
        return "read".equals(res);
    }

}
