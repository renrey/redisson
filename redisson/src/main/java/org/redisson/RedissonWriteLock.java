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
public class RedissonWriteLock extends RedissonLock implements RLock {

    protected RedissonWriteLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getRawName());
    }

    @Override
    protected String getLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }
    
    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                            // mode
                            "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                            // 无锁
                            "if (mode == false) then " +
                                  // set mode write
                                  "redis.call('hset', KEYS[1], 'mode', 'write'); " +
                                  // 线程标识、计数器
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                  // 锁key过期时间
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                  "return nil; " +
                              "end; " +
                              // 已有写锁
                              "if (mode == 'write') then " +
                                  // 重入（hash key 是自己）
                                  "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                                      // 计数器+1
                                      "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                                      // 剩余存活时间
                                      "local currentExpire = redis.call('pttl', KEYS[1]); " +
                                      // 延长过期：当前剩余时间+每次延期时间
                                      "redis.call('pexpire', KEYS[1], currentExpire + ARGV[1]); " +
                                      "return nil; " +
                                  "end; " +
                                "end;" +
                                "return redis.call('pttl', KEYS[1]);",
                        Arrays.<Object>asList(getRawName()),
                        unit.toMillis(leaseTime), getLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                // 无锁
                "if (mode == false) then " +
					// 发布解锁信息： redisson_rwlock:{key}
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end;" +
                 // 已有写锁
                "if (mode == 'write') then " +
					// 是否自己占有
                    "local lockExists = redis.call('hexists', KEYS[1], ARGV[3]); " +
                    // 不是占有线程
                    "if (lockExists == 0) then " +
                        "return nil;" +
                    // 当前持有
                    "else " +
                        // 计数器-1
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        // 减后计数器>0, 延长过期时间
                        "if (counter > 0) then " +
                            "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                            "return 0; " +
                        // 减后计数器 == 0
                        "else " +
                            // 删除锁中 线程写锁:write标识
                            "redis.call('hdel', KEYS[1], ARGV[3]); " +
                            // 如果锁的key数量==1，代表只有一个写锁（只有mode）
                            "if (redis.call('hlen', KEYS[1]) == 1) then " +
                                // 删除锁key
                                "redis.call('del', KEYS[1]); " +
                                // 发布解锁信息： redisson_rwlock:{key}
                                "redis.call('publish', KEYS[2], ARGV[1]); " +
                            // 还有读锁
                            "else " +
                                // has unlocked read-locks
                                // 改mode为read
                                "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                            "end; " +
                            "return 1; "+
                        "end; " +
                    "end; " +
                "end; "
                + "return nil;",
        Arrays.<Object>asList(getRawName(), getChannelName()),
        LockPubSub.READ_UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId));
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
              "if (redis.call('hget', KEYS[1], 'mode') == 'write') then " +
                  "redis.call('del', KEYS[1]); " +
                  "redis.call('publish', KEYS[2], ARGV[1]); " +
                  "return 1; " +
              "end; " +
              "return 0; ",
              Arrays.<Object>asList(getRawName(), getChannelName()), LockPubSub.READ_UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.HGET, getRawName(), "mode");
        String res = get(future);
        return "write".equals(res);
    }

}
