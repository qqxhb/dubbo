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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_RETRIES;
import static org.apache.dubbo.common.constants.CommonConstants.RETRIES_KEY;

/**
 * When invoke fails, log the initial error and retry other invokers (retry n
 * times, which means at most n different invokers will be invoked) Note that
 * retry causes latency.
 * <p>
 * <a href="http://en.wikipedia.org/wiki/Failover">Failover</a>
 *
 */
public class FailoverClusterInvoker<T> extends AbstractClusterInvoker<T> {

	private static final Logger logger = LoggerFactory.getLogger(FailoverClusterInvoker.class);

	public FailoverClusterInvoker(Directory<T> directory) {
		super(directory);
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Result doInvoke(Invocation invocation, final List<Invoker<T>> invokers, LoadBalance loadbalance)
			throws RpcException {
		// 因为invokers是final类型不能修改，因此重新创建一个零时变量
		List<Invoker<T>> copyInvokers = invokers;
		// 检查Invoker是不是空，是在抛出异常
		checkInvokers(copyInvokers, invocation);
		// 获取调用方法名称
		String methodName = RpcUtils.getMethodName(invocation);
		// 获取重试次数，默认1
		int len = getUrl().getMethodParameter(methodName, RETRIES_KEY, DEFAULT_RETRIES) + 1;
		if (len <= 0) {
			len = 1;
		}
		// 循环重试
		RpcException le = null; // 记录上一个调用异常
		// 记录调用过的Invoker
		List<Invoker<T>> invoked = new ArrayList<Invoker<T>>(copyInvokers.size());
		// 记录提供给地址
		Set<String> providers = new HashSet<String>(len);
		for (int i = 0; i < len; i++) {
			// 重试循环都会重新检查及列举并选则可用Invoker，避免调用过程中提供者改变（例如服务挂了）导致异常
			if (i > 0) {//表示重试
				// 检查是够已经销毁
				checkWhetherDestroyed();
				// 重新列举可用Invoker
				copyInvokers = list(invocation);
				// 校验Invoker是否为空
				checkInvokers(copyInvokers, invocation);
			}
			// 从可用Invoker中选出一个
			Invoker<T> invoker = select(loadbalance, invocation, copyInvokers, invoked);
			// 添加到已经调用过Invoker集合中
			invoked.add(invoker);
			// 设置已经调用的Invoker到上下文中
			RpcContext.getContext().setInvokers((List) invoked);
			try {
				// 执行invoke方法获取结果
				Result result = invoker.invoke(invocation);
				if (le != null && logger.isWarnEnabled()) {
					logger.warn("Although retry the method " + methodName + " in the service "
							+ getInterface().getName() + " was successful by the provider "
							+ invoker.getUrl().getAddress() + ", but there have been failed providers " + providers
							+ " (" + providers.size() + "/" + copyInvokers.size() + ") from the registry "
							+ directory.getUrl().getAddress() + " on the consumer " + NetUtils.getLocalHost()
							+ " using the dubbo version " + Version.getVersion() + ". Last error is: "
							+ le.getMessage(), le);
				}
				return result;
			} catch (RpcException e) {
				if (e.isBiz()) { // biz exception.
					throw e;
				}
				le = e;
			} catch (Throwable e) {
				le = new RpcException(e.getMessage(), e);
			} finally {
				// 缓存提供者地址
				providers.add(invoker.getUrl().getAddress());
			}
		}
		// 重试次数结束，没有返回，则表示调用失败抛出异常
		throw new RpcException(le.getCode(),
				"Failed to invoke the method " + methodName + " in the service " + getInterface().getName() + ". Tried "
						+ len + " times of the providers " + providers + " (" + providers.size() + "/"
						+ copyInvokers.size() + ") from the registry " + directory.getUrl().getAddress()
						+ " on the consumer " + NetUtils.getLocalHost() + " using the dubbo version "
						+ Version.getVersion() + ". Last error is: " + le.getMessage(),
				le.getCause() != null ? le.getCause() : le);
	}

}
