/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round robin load balance.
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "roundrobin";
    
    private static final int RECYCLE_PERIOD = 60000;
    
    protected static class WeightedRoundRobin {
    	//权重
        private int weight;
        //当前权重
        private AtomicLong current = new AtomicLong(0);
        //上次更新时间
        private long lastUpdate;
        public int getWeight() {
            return weight;
        }
        /**
         * 设置服务提供者权重,并初始化当前权重为0
         * @param weight
         */
        public void setWeight(int weight) {
            this.weight = weight;
            current.set(0);
        }
        /**
         * current += weightl;
         * @return
         */
        public long increaseCurrent() {
            return current.addAndGet(weight);
        }
        /**
         * current -= total;
         * @param total
         */
        public void sel(int total) {
            current.addAndGet(-1 * total);
        }
        public long getLastUpdate() {
            return lastUpdate;
        }
        public void setLastUpdate(long lastUpdate) {
            this.lastUpdate = lastUpdate;
        }
    }
    //最外层为服务类名 + 方法名，第二层为 url(标识某个具体服务) 到 WeightedRoundRobin 的映射关系。
    private ConcurrentMap<String, ConcurrentMap<String, WeightedRoundRobin>> methodWeightMap = new ConcurrentHashMap<String, ConcurrentMap<String, WeightedRoundRobin>>();
    //更新锁（原子操作）
    private AtomicBoolean updateLock = new AtomicBoolean();
    
    /**
     * get invoker addr list cached for specified invocation
     * <p>
     * <b>for unit test only</b>
     * 
     * @param invokers
     * @param invocation
     * @return
     */
    protected <T> Collection<String> getInvokerAddrList(List<Invoker<T>> invokers, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        Map<String, WeightedRoundRobin> map = methodWeightMap.get(key);
        if (map != null) {
            return map.keySet();
        }
        return null;
    }
    
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
    	// 调用服务可key+"."+方法名（如UserService.update）
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        // 从缓冲找中获取某个调用的<url,WeightedRoundRobin>所有映射
        ConcurrentMap<String, WeightedRoundRobin> map = methodWeightMap.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        // 总的权重
        int totalWeight = 0;
        // 最大的当前权重
        long maxCurrent = Long.MIN_VALUE;
        long now = System.currentTimeMillis();
        // 选出的服务提供者
        Invoker<T> selectedInvoker = null;
        WeightedRoundRobin selectedWRR = null;
        //遍历所有的提供者列表
        for (Invoker<T> invoker : invokers) {
        	//获取Invoker的URL标识
            String identifyString = invoker.getUrl().toIdentityString();
            //计算权重
            int weight = getWeight(invoker, invocation);
            //从缓存中获取WeightedRoundRobin
            WeightedRoundRobin weightedRoundRobin = map.get(identifyString);
            // 如果没有则新增一个，放入缓存中并赋值weightedRoundRobin
            if (weightedRoundRobin == null) {
            	// 新增WeightedRoundRobin实例并设置权重
                weightedRoundRobin = new WeightedRoundRobin();
                weightedRoundRobin.setWeight(weight);
                // 使用putIfAbsent放入缓存，防止其他线程设置
                map.putIfAbsent(identifyString, weightedRoundRobin);
                weightedRoundRobin = map.get(identifyString);
            }
            //如果权重改变了，则更新WeightedRoundRobin权重值
            if (weight != weightedRoundRobin.getWeight()) {
                weightedRoundRobin.setWeight(weight);
            }
            // 更新current权重（current += weightl）
            long cur = weightedRoundRobin.increaseCurrent();
            // 设置上次更新时间为当前时间
            weightedRoundRobin.setLastUpdate(now);
            //如果当前权重大于最大的当前权重
            if (cur > maxCurrent) {
            	//更新最大去权重
                maxCurrent = cur;
                //设置当前的Invoker为选出的Invoker
                selectedInvoker = invoker;
                //选择的权重WeightedRoundRobin
                selectedWRR = weightedRoundRobin;
            }
            //计算权重和
            totalWeight += weight;
        }
        
        // 如果未加锁并且invokers和缓存中记录的数量不同，则尝试获取锁做更新操作
        if (!updateLock.get() && invokers.size() != map.size()) {
            if (updateLock.compareAndSet(false, true)) {
                try {
                    // 复制一份新的 <url, WeightedRoundRobin>
                    ConcurrentMap<String, WeightedRoundRobin> newMap = new ConcurrentHashMap<>(map);
                    //对 <url, WeightedRoundRobin> 进行检查，过滤掉长时间未被更新的节点,，默认阈值为60秒。
                    newMap.entrySet().removeIf(item -> now - item.getValue().getLastUpdate() > RECYCLE_PERIOD);
                    methodWeightMap.put(key, newMap);
                } finally {
                    updateLock.set(false);
                }
            }
        }
        // 如果选出了提供者
        if (selectedInvoker != null) {
        	//更新当前权重减去总的权重（ current -= total）
            selectedWRR.sel(totalWeight);
            return selectedInvoker;
        }
        // 如果没有选出则直接返回第0个提供者，正常情况是不会执行的
        return invokers.get(0);
    }

}
