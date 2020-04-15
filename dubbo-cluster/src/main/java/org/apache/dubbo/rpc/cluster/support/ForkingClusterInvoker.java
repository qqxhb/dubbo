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

import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.FORKS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_FORKS;

/**
 * NOTICE! This implementation does not work well with async call.
 *
 * Invoke a specific number of invokers concurrently, usually used for demanding
 * real-time operations, but need to waste more service resources.
 *
 * <a href="http://en.wikipedia.org/wiki/Fork_(topology)">Fork</a>
 */
public class ForkingClusterInvoker<T> extends AbstractClusterInvoker<T> {

	/**
	 * Use {@link NamedInternalThreadFactory} to produce
	 * {@link org.apache.dubbo.common.threadlocal.InternalThread} which with the use
	 * of {@link org.apache.dubbo.common.threadlocal.InternalThreadLocal} in
	 * {@link RpcContext}.
	 */
	private final ExecutorService executor = Executors
			.newCachedThreadPool(new NamedInternalThreadFactory("forking-cluster-timer", true));

	public ForkingClusterInvoker(Directory<T> directory) {
		super(directory);
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Result doInvoke(final Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance)
			throws RpcException {
		try {
			// 判空校验
			checkInvokers(invokers, invocation);
			// 记录已经选择的Invoker
			final List<Invoker<T>> selected;
			// 获取fork是配置（即调用几个Invoker），默认为2
			final int forks = getUrl().getParameter(FORKS_KEY, DEFAULT_FORKS);
			// 获取超时配置，默认1秒
			final int timeout = getUrl().getParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
			// 如果forks<=0或者forks大于等于提供者数量，则选择所有的Invoker
			if (forks <= 0 || forks >= invokers.size()) {
				selected = invokers;
			} else {
				// 否则通过负载均衡算法选出forks个Invoker
				selected = new ArrayList<>();
				for (int i = 0; i < forks; i++) {
					Invoker<T> invoker = select(loadbalance, invocation, invokers, selected);
					if (!selected.contains(invoker)) {
						// Avoid add the same invoker several times.
						selected.add(invoker);
					}
				}
			}
			// 设置选出的Invoker到上下文
			RpcContext.getContext().setInvokers((List) selected);
			final AtomicInteger count = new AtomicInteger();
			final BlockingQueue<Object> ref = new LinkedBlockingQueue<>();
			// 遍历选出的Invoker
			for (final Invoker<T> invoker : selected) {
				// 将调用任务提交到线程池
				executor.execute(() -> {
					try {
						Result result = invoker.invoke(invocation);
						// 调用结果存入阻塞队列
						ref.offer(result);
					} catch (Throwable e) {
						// 记录异常数量
						int value = count.incrementAndGet();
						// 如果异常数大于等于选择的Invoker数，则将异常存入阻塞队列；表明没有一个服务成功
						if (value >= selected.size()) {
							ref.offer(e);
						}
					}
				});
			}
			try {
				// 从阻塞队列中获取结果
				Object ret = ref.poll(timeout, TimeUnit.MILLISECONDS);
				// 如果是异常则抛出异常（根据上面逻辑可知，只有调用服务都失败了才会将异常传入队列中）
				if (ret instanceof Throwable) {
					Throwable e = (Throwable) ret;
					throw new RpcException(e instanceof RpcException ? ((RpcException) e).getCode() : 0,
							"Failed to forking invoke provider " + selected
									+ ", but no luck to perform the invocation. Last error is: " + e.getMessage(),
							e.getCause() != null ? e.getCause() : e);
				}
				// 否则返回当前结果
				return (Result) ret;
			} catch (InterruptedException e) {
				throw new RpcException("Failed to forking invoke provider " + selected
						+ ", but no luck to perform the invocation. Last error is: " + e.getMessage(), e);
			}
		} finally {
			// clear attachments which is binding to current thread.
			RpcContext.getContext().clearAttachments();
		}
	}
}
