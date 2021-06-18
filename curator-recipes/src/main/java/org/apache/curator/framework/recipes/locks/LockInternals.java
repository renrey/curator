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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class LockInternals
{
    private final WatcherRemoveCuratorFramework     client;
    // 实际锁前缀= path/lock- , 例如/lock/zz/lock-
    private final String                            path;
    private final String                            basePath;
    private final LockInternalsDriver               driver;
    private final String                            lockName;
    private final AtomicReference<RevocationSpec>   revocable = new AtomicReference<RevocationSpec>(null);
    private final CuratorWatcher                    revocableWatcher = new CuratorWatcher()
    {
        @Override
        public void process(WatchedEvent event) throws Exception
        {
            if ( event.getType() == Watcher.Event.EventType.NodeDataChanged )
            {
                checkRevocableWatcher(event.getPath());
            }
        }
    };

    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            client.postSafeNotify(LockInternals.this);
        }
    };

    private volatile int    maxLeases;

    static final byte[]             REVOKE_MESSAGE = "__REVOKE__".getBytes();

    /**
     * Attempt to delete the lock node so that sequence numbers get reset
     *
     * @throws Exception errors
     */
    public void clean() throws Exception
    {
        try
        {
            client.delete().forPath(basePath);
        }
        catch ( KeeperException.BadVersionException ignore )
        {
            // ignore - another thread/process got the lock
        }
        catch ( KeeperException.NotEmptyException ignore )
        {
            // ignore - other threads/processes are waiting
        }
    }

    LockInternals(CuratorFramework client, LockInternalsDriver driver, String path, String lockName, int maxLeases)
    {
        this.driver = driver;
        this.lockName = lockName;
        this.maxLeases = maxLeases;

        this.client = client.newWatcherRemoveCuratorFramework();
        this.basePath = PathUtils.validatePath(path);
        // 实际锁前缀= path/lock-
        this.path = ZKPaths.makePath(path, lockName);
    }

    synchronized void setMaxLeases(int maxLeases)
    {
        this.maxLeases = maxLeases;
        notifyAll();
    }

    void makeRevocable(RevocationSpec entry)
    {
        revocable.set(entry);
    }

    final void releaseLock(String lockPath) throws Exception
    {
        // 移除所有watcher
        client.removeWatchers();
        revocable.set(null);
        // 删除lock节点
        deleteOurPath(lockPath);
    }

    CuratorFramework getClient()
    {
        return client;
    }

    public static Collection<String> getParticipantNodes(CuratorFramework client, final String basePath, String lockName, LockInternalsSorter sorter) throws Exception
    {
        List<String>        names = getSortedChildren(client, basePath, lockName, sorter);
        Iterable<String>    transformed = Iterables.transform
            (
                names,
                new Function<String, String>()
                {
                    @Override
                    public String apply(String name)
                    {
                        return ZKPaths.makePath(basePath, name);
                    }
                }
            );
        return ImmutableList.copyOf(transformed);
    }

    /**
     *
     * @param client
     * @param basePath 初始指定的lock路径
     * @param lockName 锁种类的节点名字前缀，如lock-
     * @param sorter
     * @return
     * @throws Exception
     */
    public static List<String> getSortedChildren(CuratorFramework client, String basePath, final String lockName, final LockInternalsSorter sorter) throws Exception
    {
        try
        {
            // 获取lock路径下的所有子节点
            List<String> children = client.getChildren().forPath(basePath);
            List<String> sortedList = Lists.newArrayList(children);
            // 所有子节点按照名称的序号来排序，升序排序
            Collections.sort
            (
                sortedList,
                new Comparator<String>()
                {
                    @Override
                    public int compare(String lhs, String rhs)
                    {
                        return sorter.fixForSorting(lhs, lockName).compareTo(sorter.fixForSorting(rhs, lockName));
                    }
                }
            );
            return sortedList;
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            return Collections.emptyList();
        }
    }

    public static List<String> getSortedChildren(final String lockName, final LockInternalsSorter sorter, List<String> children)
    {
        List<String> sortedList = Lists.newArrayList(children);
        Collections.sort
        (
            sortedList,
            new Comparator<String>()
            {
                @Override
                public int compare(String lhs, String rhs)
                {
                    return sorter.fixForSorting(lhs, lockName).compareTo(sorter.fixForSorting(rhs, lockName));
                }
            }
        );
        return sortedList;
    }

    List<String> getSortedChildren() throws Exception
    {
        return getSortedChildren(client, basePath, lockName, driver);
    }

    String getLockName()
    {
        return lockName;
    }

    LockInternalsDriver getDriver()
    {
        return driver;
    }

    String attemptLock(long time, TimeUnit unit, byte[] lockNodeBytes) throws Exception
    {
        // 开始时间，即当前时间
        final long      startMillis = System.currentTimeMillis();
        // 需要的等待时长
        final Long      millisToWait = (unit != null) ? unit.toMillis(time) : null;
        // 初始为null
        final byte[]    localLockNodeBytes = (revocable.get() != null) ? new byte[0] : lockNodeBytes;
        int             retryCount = 0;

        String          ourPath = null;
        boolean         hasTheLock = false;
        // 是否完成标识，主要用于重试
        boolean         isDone = false;
        // 重试
        while ( !isDone )
        {
            isDone = true;

            try
            {
                /**
                 * 创建lock节点（znode，临时顺序），ourPath: 创建后znode路径（临时顺序节点）
				 * localLockNodeBytes初始是null
                 * path ： 参数path/lock- , 例如/lock/zz/lock-
                 */
                ourPath = driver.createsTheLock(client, path, localLockNodeBytes);
                // 进行是否获取锁成功的判断，如果成功就会返回，不成功会挂起监听上一节点再重试，直到成功为止（如果超时时间则会终止）
                hasTheLock = internalLockLoop(startMillis, millisToWait, ourPath);
            }
            catch ( KeeperException.NoNodeException e )
            {
                // gets thrown by StandardLockInternalsDriver when it can't find the lock node
                // this can happen when the session expires, etc. So, if the retry allows, just try it all again
                // 重试是否超过要求
                if ( client.getZookeeperClient().getRetryPolicy().allowRetry(retryCount++, System.currentTimeMillis() - startMillis, RetryLoop.getDefaultRetrySleeper()) )
                {
                    isDone = false;
                }
                else
                {
                    throw e;
                }
            }
        }
        // 获取成功，返回lock节点路径
        if ( hasTheLock )
        {
            return ourPath;
        }

        return null;
    }

    private void checkRevocableWatcher(String path) throws Exception
    {
        RevocationSpec  entry = revocable.get();
        if ( entry != null )
        {
            try
            {
                byte[]      bytes = client.getData().usingWatcher(revocableWatcher).forPath(path);
                if ( Arrays.equals(bytes, REVOKE_MESSAGE) )
                {
                    entry.getExecutor().execute(entry.getRunnable());
                }
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                // ignore
            }
        }
    }

    private boolean internalLockLoop(long startMillis, Long millisToWait, String ourPath) throws Exception
    {
        boolean     haveTheLock = false;
        boolean     doDelete = false;
        try
        {
            if ( revocable.get() != null )
            {
                client.getData().usingWatcher(revocableWatcher).forPath(ourPath);
            }
            // 当前的客户端对象还在使用 and 未实际拿到lock，
            // 正常执行
            while ( (client.getState() == CuratorFrameworkState.STARTED) && !haveTheLock )
            {
                // 获取指定lock路径（/lock/zz）下的子节点，且以序号拍好序（升序），例如/lock/zz/lock-0000、/lock/zz/lock-0001 、/lock/zz/lock-0002
                List<String>        children = getSortedChildren();
                // 获取节点名称，截取掉父节点路径
                String              sequenceNodeName = ourPath.substring(basePath.length() + 1); // +1 to include the slash

				// 判断是否需要加锁、加watcher
                PredicateResults    predicateResults = driver.getsTheLock(client, children, sequenceNodeName, maxLeases);
                // 加锁成功
                if ( predicateResults.getsTheLock() )
                {
                    haveTheLock = true;
                }
                // 加锁失败
                else
                {
                    // 需要监听的节点完整路径
                    String  previousSequencePath = basePath + "/" + predicateResults.getPathToWatch();

                    synchronized(this)
                    {
                        try
                        {
                            // use getData() instead of exists() to avoid leaving unneeded watchers which is a type of resource leak
                            // 对上一个节点添加watcher
                            client.getData().usingWatcher(watcher).forPath(previousSequencePath);
                            // 有限时等待
                            if ( millisToWait != null )
                            {
                                // 需要等待的时间，原来等待时长-执行zk相关的时间
                                millisToWait -= (System.currentTimeMillis() - startMillis);
                                startMillis = System.currentTimeMillis();
                                if ( millisToWait <= 0 )
                                {
                                    doDelete = true;    // timed out - delete our node
                                    break;
                                }
                                // 等待一定时间
                                wait(millisToWait);
                            }
                            // 无限时等待
                            else
                            {
                                wait();
                            }
                        }
                        catch ( KeeperException.NoNodeException e )
                        {
                            // it has been deleted (i.e. lock released). Try to acquire again
                        }
                    }
                }
            }
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            doDelete = true;
            throw e;
        }
        finally
        {
            // 一定会删除lock节点
            if ( doDelete )
            {
                deleteOurPath(ourPath);
            }
        }
        return haveTheLock;
    }

    private void deleteOurPath(String ourPath) throws Exception
    {
        try
        {
            client.delete().guaranteed().forPath(ourPath);
        }
        catch ( KeeperException.NoNodeException e )
        {
            // ignore - already deleted (possibly expired session, etc.)
        }
    }
}
