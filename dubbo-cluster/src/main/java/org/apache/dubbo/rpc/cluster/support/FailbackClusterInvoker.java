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
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_FAILBACK_TIMES;
import static org.apache.dubbo.common.constants.CommonConstants.RETRIES_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_FAILBACK_TASKS;
import static org.apache.dubbo.rpc.cluster.Constants.FAIL_BACK_TASKS_KEY;

/**
 * When fails, record failure requests and schedule for retry on a regular
 * interval. Especially useful for services of notification.
 *
 * <a href="http://en.wikipedia.org/wiki/Failback">Failback</a>
 */
public class FailbackClusterInvoker<T> extends AbstractClusterInvoker<T> {

	private static final Logger logger = LoggerFactory.getLogger(FailbackClusterInvoker.class);

	private static final long RETRY_FAILED_PERIOD = 5;

	private final int retries;

	private final int failbackTasks;

	private volatile Timer failTimer;

	public FailbackClusterInvoker(Directory<T> directory) {
		super(directory);

		int retriesConfig = getUrl().getParameter(RETRIES_KEY, DEFAULT_FAILBACK_TIMES);
		if (retriesConfig <= 0) {
			retriesConfig = DEFAULT_FAILBACK_TIMES;
		}
		int failbackTasksConfig = getUrl().getParameter(FAIL_BACK_TASKS_KEY, DEFAULT_FAILBACK_TASKS);
		if (failbackTasksConfig <= 0) {
			failbackTasksConfig = DEFAULT_FAILBACK_TASKS;
		}
		retries = retriesConfig;
		failbackTasks = failbackTasksConfig;
	}

	private void addFailed(LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers,
			Invoker<T> lastInvoker) {
		// 计数器为空，通过双重检查创建一个哈希轮计时器实例
		if (failTimer == null) {
			synchronized (this) {
				if (failTimer == null) {
					failTimer = new HashedWheelTimer(new NamedThreadFactory("failback-cluster-timer", true), 1,
							TimeUnit.SECONDS, 32, failbackTasks);
				}
			}
		}
		// 新建一个重试任务
		RetryTimerTask retryTimerTask = new RetryTimerTask(loadbalance, invocation, invokers, lastInvoker, retries,
				RETRY_FAILED_PERIOD);
		try {
			// 指定的RetryTimerTask将在指定延迟（5秒）之后执行。
			failTimer.newTimeout(retryTimerTask, RETRY_FAILED_PERIOD, TimeUnit.SECONDS);
		} catch (Throwable e) {
			logger.error(
					"Failback background works error,invocation->" + invocation + ", exception: " + e.getMessage());
		}
	}

	@Override
	protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance)
			throws RpcException {
		Invoker<T> invoker = null;
		try {
			// 判空校验
			checkInvokers(invokers, invocation);
			// 选择可用服务
			invoker = select(loadbalance, invocation, invokers, null);
			// 执行调用
			return invoker.invoke(invocation);
		} catch (Throwable e) {
			logger.error("Failback to invoke method " + invocation.getMethodName()
					+ ", wait for retry in background. Ignored exception: " + e.getMessage() + ", ", e);
			// 记录异常调用
			addFailed(loadbalance, invocation, invokers, invoker);
			// 返回一个空异步结果给服务消费者
			return AsyncRpcResult.newDefaultAsyncResult(null, null, invocation);
		}
	}

	@Override
	public void destroy() {
		super.destroy();
		if (failTimer != null) {
			failTimer.stop();
		}
	}

	/**
	 * RetryTimerTask
	 */
	private class RetryTimerTask implements TimerTask {
		private final Invocation invocation;
		private final LoadBalance loadbalance;
		private final List<Invoker<T>> invokers;
		private final int retries;
		private final long tick;
		private Invoker<T> lastInvoker;
		private int retryTimes = 0;

		RetryTimerTask(LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers,
				Invoker<T> lastInvoker, int retries, long tick) {
			this.loadbalance = loadbalance;
			this.invocation = invocation;
			this.invokers = invokers;
			this.retries = retries;
			this.tick = tick;
			this.lastInvoker = lastInvoker;
		}

		@Override
		public void run(Timeout timeout) {
			// 重试执行是调用
			try {
				// 选择上次执行的Invoker，进行远程调用
				Invoker<T> retryInvoker = select(loadbalance, invocation, invokers,
						Collections.singletonList(lastInvoker));
				lastInvoker = retryInvoker;
				retryInvoker.invoke(invocation);
			} catch (Throwable e) {
				logger.error("Failed retry to invoke method " + invocation.getMethodName() + ", waiting again.", e);
				if ((++retryTimes) >= retries) {
					// 如果超过了重试次数，则答应错误日志
					logger.error("Failed retry times exceed threshold (" + retries
							+ "), We have to abandon, invocation->" + invocation);
				} else {
					// 否则重新将任务存入调度队列中
					rePut(timeout);
				}
			}
		}

		private void rePut(Timeout timeout) {
			if (timeout == null) {
				return;
			}
			// 获取计时器，如果已经停止或取消则返回
			Timer timer = timeout.timer();
			if (timer.isStop() || timeout.isCancelled()) {
				return;
			}
			// 将当前定时任务在放入延时调用队列中
			timer.newTimeout(timeout.task(), tick, TimeUnit.SECONDS);
		}
	}
}
