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
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;

import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_SERVICE_REFERENCE_PATH;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_WARMUP;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_WEIGHT;
import static org.apache.dubbo.rpc.cluster.Constants.WARMUP_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.WEIGHT_KEY;

/**
 * AbstractLoadBalance
 */
public abstract class AbstractLoadBalance implements LoadBalance {
	/**
	 * Calculate the weight according to the uptime proportion of warmup time the
	 * new weight will be within 1(inclusive) to weight(inclusive)
	 *
	 * @param uptime the uptime in milliseconds
	 * @param warmup the warmup time in milliseconds
	 * @param weight the weight of an invoker
	 * @return weight which takes warmup into account
	 */
	/**
	 * 该过程主要用于保证当服务运行时长小于服务预热时间时，对服务进行降权，避免让服务在启动之初就处于高负载状态。 服务预热是一个优化手段，与此类似的还有 JVM
	 * 预热。 主要目的是让服务启动后“低功率”运行一段时间，使其效率慢慢提升至最佳状态。
	 * 
	 * @param uptime
	 * @param warmup
	 * @param weight
	 * @return
	 */
	static int calculateWarmupWeight(int uptime, int warmup, int weight) {
		// 根据运行时间和预热时间的比例，计算权重，计算结果介于1-weight之间
		int ww = (int) (uptime / ((float) warmup / weight));
		return ww < 1 ? 1 : (Math.min(ww, weight));
	}

	@Override
	public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
		// 集合是如果没有可用执行者直接返回空
		if (CollectionUtils.isEmpty(invokers)) {
			return null;
		}
		// 集合中如果只有一个可用的执行者，直接返回，不需要负载均衡介入
		if (invokers.size() == 1) {
			return invokers.get(0);
		}
		// 调用子类实现的具体负载均衡算法进行选择
		return doSelect(invokers, url, invocation);
	}

	protected abstract <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation);

	/**
	 * 根据预热时间及正常运行时间计算调用程序的权重
	 *
	 */
	int getWeight(Invoker<?> invoker, Invocation invocation) {
		int weight;
		// 取出调用URL
		URL url = invoker.getUrl();
		// 多注册中心的权重计算方式（如果调用的是注册中心服务则从URL中获取(registry.weight)权重，默认100）
		if (REGISTRY_SERVICE_REFERENCE_PATH.equals(url.getServiceInterface())) {
			weight = url.getParameter(REGISTRY_KEY + "." + WEIGHT_KEY, DEFAULT_WEIGHT);
		} else {
			// 不是调用注册中心服务，则用具体调用服务名从URL中获取权重值（默认也是100）
			weight = url.getMethodParameter(invocation.getMethodName(), WEIGHT_KEY, DEFAULT_WEIGHT);
			if (weight > 0) {
				// 从URL中获取时间戳（timestamp）的值，默认为0
				long timestamp = invoker.getUrl().getParameter(TIMESTAMP_KEY, 0L);
				if (timestamp > 0L) {
					// 计算服务运行时间
					long uptime = System.currentTimeMillis() - timestamp;
					if (uptime < 0) {// 运行时间小于0则返回权重1
						return 1;
					}
					// 获取服务预热时间，默认为10分钟
					int warmup = invoker.getUrl().getParameter(WARMUP_KEY, DEFAULT_WARMUP);
					// 如果服务运行时间小于预热时间，则重新计算服务权重，即降权
					if (uptime > 0 && uptime < warmup) {
						weight = calculateWarmupWeight((int) uptime, warmup, weight);
					}
				}
			}
		}
		return Math.max(weight, 0);
	}
}
