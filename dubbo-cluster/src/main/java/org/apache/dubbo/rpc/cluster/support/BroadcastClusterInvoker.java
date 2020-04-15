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

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;

/**
 * BroadcastClusterInvoker
 *
 */
public class BroadcastClusterInvoker<T> extends AbstractClusterInvoker<T> {

	private static final Logger logger = LoggerFactory.getLogger(BroadcastClusterInvoker.class);

	public BroadcastClusterInvoker(Directory<T> directory) {
		super(directory);
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Result doInvoke(final Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance)
			throws RpcException {
		// 检查Invoker是不是空，是在抛出异常
		checkInvokers(invokers, invocation);
		// 设置调用的Invoker到上下文中
		RpcContext.getContext().setInvokers((List) invokers);
		// 记录异常
		RpcException exception = null;
		Result result = null;
		// 遍历 Invoker 列表，逐个调用远程服务
		for (Invoker<T> invoker : invokers) {
			try {
				result = invoker.invoke(invocation);
			} catch (RpcException e) {
				exception = e;
				logger.warn(e.getMessage(), e);
			} catch (Throwable e) {
				exception = new RpcException(e.getMessage(), e);
				logger.warn(e.getMessage(), e);
			}
		}
		// 有一个失败则抛出异常
		if (exception != null) {
			throw exception;
		}
		return result;
	}

}
