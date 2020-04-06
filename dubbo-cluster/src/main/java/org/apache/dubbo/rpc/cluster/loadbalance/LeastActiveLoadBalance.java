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
import org.apache.dubbo.rpc.RpcStatus;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * LeastActiveLoadBalance
 * <p>
 * Filter the number of invokers with the least number of active calls and count the weights and quantities of these invokers.
 * If there is only one invoker, use the invoker directly;
 * if there are multiple invokers and the weights are not the same, then random according to the total weight;
 * if there are multiple invokers and the same weight, then randomly called.
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // 服务提供者（Invoker）数量
        int length = invokers.size();
        // 最小活跃数
        int leastActive = -1;
        // 最小活跃数Invoker的数量（多个Invoker的最小活跃数相同）
        int leastCount = 0;
        // 最小活跃数Invoker对应的下标
        int[] leastIndexes = new int[length];
        // 每个Invoker的权重
        int[] weights = new int[length];
        // 所有最不活跃调用程序的预热权重之和
        int totalWeight = 0;
        // 第一个最小活跃Invoker的权重
        int firstWeight = 0;
        // 所有最小活跃Invoker的权重是否一致
        boolean sameWeight = true;


        // 找出所有最小活跃Invoker
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            // 获取Invoker的活跃数
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive();
            // 获取Invoker的权重，默认100（该方法在抽象类中实现）
            int afterWarmup = getWeight(invoker, invocation);
            // 保存当前Invoker的权重值
            weights[i] = afterWarmup;
            // 如果当前Invoker是活跃数最小的Invoker（最小活跃数为-1或者比当前Invoker活跃数大）
            if (leastActive == -1 || active < leastActive) {
                //更新最小小活跃数为当前的Invoker活跃数
                leastActive = active;
                // 重置最小活跃Invoker数量为-1
                leastCount = 1;
                // 设置当前Invoker为最小活跃Invoker的第一个值
                leastIndexes[0] = i;
                // 重置总权重为当前Invoker权重
                totalWeight = afterWarmup;
                // 重置第一个最小活跃Invoker的权重为当前Invoker权重
                firstWeight = afterWarmup;
                //设置所有Invoker权重相同
                sameWeight = true;
                //如果当前Invoker活跃数和记录中的最小活跃数相同
            } else if (active == leastActive) {
                //记录大当前Invoker到最小Invoker下标数组
                leastIndexes[leastCount++] = i;
                // 总权重值加上当前的Invoker权重
                totalWeight += afterWarmup;
                // 之前所有最小Invoker的权重相同并当前权重和记录中的最下权重不同，则更新sameWeight为false
                if (sameWeight && i > 0
                        && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }
        if (leastCount == 1) {
            // 如果最小活跃的Invoker只有一个，则直接返回
            return invokers.get(leastIndexes[0]);
        }
        if (!sameWeight && totalWeight > 0) {
            // 如果所有最小活跃的权重不都相同并且总权重大于0, 则在[0-totalWeight）范围随机一个数
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            // 根据随机值，找出对应区间的Invoker返回
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexes[i];
                offsetWeight -= weights[leastIndex];
                if (offsetWeight < 0) {
                    return invokers.get(leastIndex);
                }
            }
        }
        // 如果所有最小活跃Invoker权重相同或者总权重等于0，则随机返回一个Invoker
        return invokers.get(leastIndexes[ThreadLocalRandom.current().nextInt(leastCount)]);
    }
}
