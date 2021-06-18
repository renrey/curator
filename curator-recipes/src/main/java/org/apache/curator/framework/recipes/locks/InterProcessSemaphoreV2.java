/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.recipes.locks;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * A counting semaphore that works across JVMs. All processes
 * in all JVMs that use the same lock path will achieve an inter-process limited set of leases.
 * Further, this semaphore is mostly "fair" - each user will get a lease in the order requested
 * (from ZK's point of view).
 * </p>
 * <p>
 * There are two modes for determining the max leases for the semaphore. In the first mode the
 * max leases is a convention maintained by the users of a given path. In the second mode a
 * {@link SharedCountReader} is used as the method for semaphores of a given path to determine
 * the max leases.
 * </p>
 * <p>
 * If a {@link SharedCountReader} is <b>not</b> used, no internal checks are done to prevent
 * Process A acting as if there are 10 leases and Process B acting as if there are 20. Therefore,
 * make sure that all instances in all processes use the same numberOfLeases value.
 * </p>
 * <p>
 * The various acquire methods return {@link Lease} objects that represent acquired leases. Clients
 * must take care to close lease objects  (ideally in a <code>finally</code>
 * block) else the lease will be lost. However, if the client session drops (crash, etc.),
 * any leases held by the client are automatically closed and made available to other clients.
 * </p>
 * <p>
 * Thanks to Ben Bangert (ben@groovie.org) for the algorithm used.
 * </p>
 */
public class InterProcessSemaphoreV2
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final InterProcessMutex lock;
    private final WatcherRemoveCuratorFramework client;
    private final String leasesPath;
    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            client.postSafeNotify(InterProcessSemaphoreV2.this);
        }
    };

    private volatile byte[] nodeData;
    private volatile int maxLeases;

    private static final String LOCK_PARENT = "locks";
    private static final String LEASE_PARENT = "leases";
    private static final String LEASE_BASE_NAME = "lease-";
    public static final Set<String> LOCK_SCHEMA = Sets.newHashSet(
            LOCK_PARENT,
            LEASE_PARENT
    );

    /**
     * @param client    the client
     * @param path      path for the semaphore
     * @param maxLeases the max number of leases to allow for this instance
     */
    public InterProcessSemaphoreV2(CuratorFramework client, String path, int maxLeases)
    {
        this(client, path, maxLeases, null);
    }

    /**
     * @param client the client
     * @param path   path for the semaphore
     * @param count  the shared count to use for the max leases
     */
    public InterProcessSemaphoreV2(CuratorFramework client, String path, SharedCountReader count)
    {
        this(client, path, 0, count);
    }

    private InterProcessSemaphoreV2(CuratorFramework client, String path, int maxLeases, SharedCountReader count)
    {
        this.client = client.newWatcherRemoveCuratorFramework();
        path = PathUtils.validatePath(path);
        // InterProcessMutex锁, 路径为path/locks, 如/semaphore1/locks
        lock = new InterProcessMutex(client, ZKPaths.makePath(path, LOCK_PARENT));
        // 需要的资源数量
        this.maxLeases = (count != null) ? count.getCount() : maxLeases;
        // leases的路径：路径为path/leases,如/semaphore1/leases
        leasesPath = ZKPaths.makePath(path, LEASE_PARENT);

        if ( count != null )
        {
            count.addListener
                (
                    new SharedCountListener()
                    {
                        @Override
                        public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception
                        {
                            InterProcessSemaphoreV2.this.maxLeases = newCount;
                            client.postSafeNotify(InterProcessSemaphoreV2.this);
                        }

                        @Override
                        public void stateChanged(CuratorFramework client, ConnectionState newState)
                        {
                            // no need to handle this here - clients should set their own connection state listener
                        }
                    }
                );
        }
    }

    /**
     * Set the data to put for the node created by this semaphore. This must be called prior to calling one
     * of the acquire() methods.
     *
     * @param nodeData node data
     */
    public void setNodeData(byte[] nodeData)
    {
        this.nodeData = (nodeData != null) ? Arrays.copyOf(nodeData, nodeData.length) : null;
    }

    /**
     * Return a list of all current nodes participating in the semaphore
     *
     * @return list of nodes
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<String> getParticipantNodes() throws Exception
    {
        return client.getChildren().forPath(leasesPath);
    }

    /**
     * Convenience method. Closes all leases in the given collection of leases
     *
     * @param leases leases to close
     */
    public void returnAll(Collection<Lease> leases)
    {
        for ( Lease l : leases )
        {
            CloseableUtils.closeQuietly(l);
        }
    }

    /**
     * Convenience method. Closes the lease
     *
     * @param lease lease to close
     */
    public void returnLease(Lease lease)
    {
        CloseableUtils.closeQuietly(lease);
    }

    /**
     * <p>Acquire a lease. If no leases are available, this method blocks until either the maximum
     * number of leases is increased or another client/process closes a lease.</p>
     * <p>The client must close the lease when it is done with it. You should do this in a
     * <code>finally</code> block.</p>
     *
     * @return the new lease
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Lease acquire() throws Exception
    {
        Collection<Lease> leases = acquire(1, 0, null);
        return leases.iterator().next();
    }

    /**
     * <p>Acquire <code>qty</code> leases. If there are not enough leases available, this method
     * blocks until either the maximum number of leases is increased enough or other clients/processes
     * close enough leases.</p>
     * <p>The client must close the leases when it is done with them. You should do this in a
     * <code>finally</code> block. NOTE: You can use {@link #returnAll(Collection)} for this.</p>
     *
     * @param qty number of leases to acquire
     * @return the new leases
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<Lease> acquire(int qty) throws Exception
    {
        return acquire(qty, 0, null);
    }

    /**
     * <p>Acquire a lease. If no leases are available, this method blocks until either the maximum
     * number of leases is increased or another client/process closes a lease. However, this method
     * will only block to a maximum of the time parameters given.</p>
     * <p>The client must close the lease when it is done with it. You should do this in a
     * <code>finally</code> block.</p>
     *
     * @param time time to wait
     * @param unit time unit
     * @return the new lease or null if time ran out
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Lease acquire(long time, TimeUnit unit) throws Exception
    {
        Collection<Lease> leases = acquire(1, time, unit);
        return (leases != null) ? leases.iterator().next() : null;
    }

    /**
     * <p>Acquire <code>qty</code> leases. If there are not enough leases available, this method
     * blocks until either the maximum number of leases is increased enough or other clients/processes
     * close enough leases. However, this method will only block to a maximum of the time
     * parameters given. If time expires before all leases are acquired, the subset of acquired
     * leases are automatically closed.</p>
     * <p>The client must close the leases when it is done with them. You should do this in a
     * <code>finally</code> block. NOTE: You can use {@link #returnAll(Collection)} for this.</p>
     *
     * @param qty  number of leases to acquire 需要获取的资源数
     * @param time time to wait
     * @param unit time unit
     * @return the new leases or null if time ran out
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<Lease> acquire(int qty, long time, TimeUnit unit) throws Exception
    {
        long startMs = System.currentTimeMillis();
        boolean hasWait = (unit != null);
        // 需要等待的时间
        long waitMs = hasWait ? TimeUnit.MILLISECONDS.convert(time, unit) : 0;

        Preconditions.checkArgument(qty > 0, "qty cannot be 0");

        ImmutableList.Builder<Lease> builder = ImmutableList.builder();
        boolean success = false;
        try
        {
            // 需要获取的资源数-1，然后获取资源
            while ( qty-- > 0 )
            {
                int retryCount = 0;
                long startMillis = System.currentTimeMillis();
                boolean isDone = false;
                // 重试循环
                while ( !isDone )
                {
                    // 进行获取资源
                    // 下面是针对操作结果的执行
                    switch ( internalAcquire1Lease(builder, startMs, hasWait, waitMs) )
                    {
                        // 成功，跳出获取当前资源的循环，进行获取下个资源的
                        case CONTINUE:
                        {
                            isDone = true;
                            break;
                        }
                        // 连接失败，直接返回null
                        case RETURN_NULL:
                        {
                            return null;
                        }
                        // 当前资源获取失败重试
                        case RETRY_DUE_TO_MISSING_NODE:
                        {
                            // gets thrown by internalAcquire1Lease when it can't find the lock node
                            // this can happen when the session expires, etc. So, if the retry allows, just try it all again
                            // 重试策略判断
                            if ( !client.getZookeeperClient().getRetryPolicy().allowRetry(retryCount++, System.currentTimeMillis() - startMillis, RetryLoop.getDefaultRetrySleeper()) )
                            {
                                throw new KeeperException.NoNodeException("Sequential path not found - possible session loss");
                            }
                            // try again
                            break;
                        }
                    }
                }
            }
            success = true;
        }
        finally
        {
            if ( !success )
            {
                returnAll(builder.build());
            }
        }

        return builder.build();
    }

    private enum InternalAcquireResult
    {
        CONTINUE,
        RETURN_NULL,
        RETRY_DUE_TO_MISSING_NODE
    }

    static volatile CountDownLatch debugAcquireLatch = null;
    static volatile CountDownLatch debugFailedGetChildrenLatch = null;
    volatile CountDownLatch debugWaitLatch = null;

    /**
     * Semaphore中获取1ge资源
     * @param builder
     * @param startMs
     * @param hasWait
     * @param waitMs
     * @return
     * @throws Exception
     */
    private InternalAcquireResult internalAcquire1Lease(ImmutableList.Builder<Lease> builder, long startMs, boolean hasWait, long waitMs) throws Exception
    {
        // 连接关闭等外部情况
        if ( client.getState() != CuratorFrameworkState.STARTED )
        {
            return InternalAcquireResult.RETURN_NULL;
        }

        /**
         * 可以看到其实还是使用单个InterProcessMutex来进行获取
         */
        // 有等待时间获取
        if ( hasWait )
        {
            long thisWaitMs = getThisWaitMs(startMs, waitMs);
            if ( !lock.acquire(thisWaitMs, TimeUnit.MILLISECONDS) )
            {
                return InternalAcquireResult.RETURN_NULL;
            }
        }
        // 无等待时间获取
        else
        {
            lock.acquire();
        }

        Lease lease = null;
        boolean success = false;

        try
        {
            PathAndBytesable<String> createBuilder = client.create().creatingParentContainersIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL);
            // 前缀：path/leases/lease-,如/semaphore1/leases/lease-
            // 创建lease节点，临时顺序节点,/semaphore1/leases/lease-0000
            String path = (nodeData != null) ? createBuilder.forPath(ZKPaths.makePath(leasesPath, LEASE_BASE_NAME), nodeData) : createBuilder.forPath(ZKPaths.makePath(leasesPath, LEASE_BASE_NAME));
            String nodeName = ZKPaths.getNodeFromPath(path);
            lease = makeLease(path);

            // dedbug使用，忽略
            if ( debugAcquireLatch != null )
            {
                debugAcquireLatch.await();
            }

            try
            {
                synchronized(this)
                {
                    for(;;)
                    {
                        List<String> children;
                        try
                        {
                            // 对leases目录(path/leases)下的所有节点添加watcher
                            // 向所有path/leases/lease-加watcher
                            children = client.getChildren().usingWatcher(watcher).forPath(leasesPath);
                        }
                        catch ( Exception e )
                        {
                            if ( debugFailedGetChildrenLatch != null )
                            {
                                debugFailedGetChildrenLatch.countDown();
                            }
                            throw e;
                        }
                        if ( !children.contains(nodeName) )
                        {
                            log.error("Sequential path not found: " + path);
                            return InternalAcquireResult.RETRY_DUE_TO_MISSING_NODE;
                        }
                        // lease节点数量 <= maxLeases，获取成功，退出循环
                        if ( children.size() <= maxLeases )
                        {
                            break;
                        }
                        /**
                         * 获取资源失败，挂起，等待唤醒重试
                         */
                        if ( hasWait )
                        {
                            long thisWaitMs = getThisWaitMs(startMs, waitMs);
                            if ( thisWaitMs <= 0 )
                            {
                                return InternalAcquireResult.RETURN_NULL;
                            }
                            if ( debugWaitLatch != null )
                            {
                                debugWaitLatch.countDown();
                            }
                            wait(thisWaitMs);
                        }
                        else
                        {
                            if ( debugWaitLatch != null )
                            {
                                debugWaitLatch.countDown();
                            }
                            wait();
                        }
                    }
                    success = true;
                }
            }
            finally
            {
                // 获取资源不成功，把所有lease的关闭
                if ( !success )
                {
                    returnLease(lease);
                }
                client.removeWatchers();
            }
        }
        finally
        {
            /**
             * 注意，获取到资源成功后，会释放lock
             * 获取失败，也释放lock
             */
            lock.release();
        }
        /**
         * 可以理解过程为：
         * 1. 先获取独占锁（/locks/lock-000X）
         * 2. 获取成功后，进行lease资源操作，先新增lease节点（/leases/lease-000X）
         * 3. 判断加后lease节点数量是否 <= maxLeases(最大可获取的资源数)
         *    符合就获取资源成功，
         *    不符合获取资源失败：对所有lease节点加watcher，挂起等待，等待有lease释放，watcher监听到把线程唤醒
         * 4. 把lock锁释放（节点删除）
         * 思路就是：
         * 通过独占锁，使得只有一个线程可以进行lease相关操作。
         * lease的操作，先加lease节点，然后当前lease节点是否不超过需求的总数量，不超过就是成功，超过就挂起，通过watcher等待节点减少，唤醒
         */
        builder.add(Preconditions.checkNotNull(lease));
        return InternalAcquireResult.CONTINUE;
    }

    private long getThisWaitMs(long startMs, long waitMs)
    {
        long elapsedMs = System.currentTimeMillis() - startMs;
        return waitMs - elapsedMs;
    }

    private Lease makeLease(final String path)
    {
        return new Lease()
        {
            @Override
            public void close() throws IOException
            {
                try
                {
                    client.delete().guaranteed().forPath(path);
                }
                catch ( KeeperException.NoNodeException e )
                {
                    log.warn("Lease already released", e);
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    throw new IOException(e);
                }
            }

            @Override
            public byte[] getData() throws Exception
            {
                return client.getData().forPath(path);
            }

            @Override
            public String getNodeName() {
                return ZKPaths.getNodeFromPath(path);
            }
        };
    }
}
