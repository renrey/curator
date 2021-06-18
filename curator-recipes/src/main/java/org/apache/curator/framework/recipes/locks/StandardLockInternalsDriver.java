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

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class StandardLockInternalsDriver implements LockInternalsDriver
{
    static private final Logger log = LoggerFactory.getLogger(StandardLockInternalsDriver.class);

    /**
     * 判断是否获取到锁
     * @param client
     * @param children 父目录（lock目录）下所有子节点，按升序排序
     * @param sequenceNodeName 当前节点名称（无父路径）
     * @param maxLeases zk中最大需要释放（持有锁）资源数量，可以看成可以获取lock的最大下标范围
     * @return
     * @throws Exception
     */
    @Override
    public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception
    {
        // 当前节点在现有lock节点中的位置
        int             ourIndex = children.indexOf(sequenceNodeName);
        // 是否存在zk中，健壮性校验
        validateOurIndex(sequenceNodeName, ourIndex);
        /**
         * 是否获取到lock
         * 互斥锁，maxLeases =1,只需一个资源， 需要ourIndex<1, 就是第一个节点
         */
        boolean         getsTheLock = ourIndex < maxLeases;
        /**
         * 是否需要添加watcher
         * 没获取锁成功，需要监听上一个节点
         */
        String          pathToWatch = getsTheLock ? null : children.get(ourIndex - maxLeases);

        return new PredicateResults(pathToWatch, getsTheLock);
    }

    /**
     * 实际在zk中创建lock节点
     * @param client
     * @param path
     * @param lockNodeBytes
     * @return
     * @throws Exception
     */
    @Override
    public String createsTheLock(CuratorFramework client, String path, byte[] lockNodeBytes) throws Exception
    {
        String ourPath;
        /**
         * 区别就是创建znode，放不放数据
         */
        if ( lockNodeBytes != null )
        {
            ourPath = client.create().creatingParentContainersIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, lockNodeBytes);
        }
        // 一开始是null,进入这里
        else
        {
            // 就是创建znode，类型为EPHEMERAL_SEQUENTIAL临时顺序
            // 例如/lock/zz/lock-0000，类型是临时顺序
            ourPath = client.create().creatingParentContainersIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path);
        }
        // 返回实际创建的路径，路径带有顺序序号
        return ourPath;
    }


    @Override
    public String fixForSorting(String str, String lockName)
    {
        return standardFixForSorting(str, lockName);
    }

    public static String standardFixForSorting(String str, String lockName)
    {
        int index = str.lastIndexOf(lockName);
        if ( index >= 0 )
        {
            index += lockName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }

    static void validateOurIndex(String sequenceNodeName, int ourIndex) throws KeeperException
    {
        if ( ourIndex < 0 )
        {
            throw new KeeperException.NoNodeException("Sequential path not found: " + sequenceNodeName);
        }
    }
}
