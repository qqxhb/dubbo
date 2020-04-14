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
package org.apache.dubbo.common.bytecode;

import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.ReflectUtils;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.dubbo.common.constants.CommonConstants.MAX_PROXY_COUNT;

/**
 * Proxy.
 */

public abstract class Proxy {
	public static final InvocationHandler RETURN_NULL_INVOKER = (proxy, method, args) -> null;
	public static final InvocationHandler THROW_UNSUPPORTED_INVOKER = new InvocationHandler() {
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) {
			throw new UnsupportedOperationException("Method [" + ReflectUtils.getName(method) + "] unimplemented.");
		}
	};
	private static final AtomicLong PROXY_CLASS_COUNTER = new AtomicLong(0);
	private static final String PACKAGE_NAME = Proxy.class.getPackage().getName();
	private static final Map<ClassLoader, Map<String, Object>> PROXY_CACHE_MAP = new WeakHashMap<ClassLoader, Map<String, Object>>();

	private static final Object PENDING_GENERATION_MARKER = new Object();

	protected Proxy() {
	}

	/**
	 * Get proxy.
	 *
	 * @param ics interface class array.
	 * @return Proxy instance.
	 */
	public static Proxy getProxy(Class<?>... ics) {
		// 调用重载
		return getProxy(ClassUtils.getClassLoader(Proxy.class), ics);
	}

	/**
	 * Get proxy.
	 *
	 * @param cl  class loader.
	 * @param ics interface class array.
	 * @return Proxy instance.
	 */
	public static Proxy getProxy(ClassLoader cl, Class<?>... ics) {
		// 接口数量限制65535
		if (ics.length > MAX_PROXY_COUNT) {
			throw new IllegalArgumentException("interface limit exceeded");
		}

		StringBuilder sb = new StringBuilder();
		// 遍历接口列表
		for (int i = 0; i < ics.length; i++) {
			String itf = ics[i].getName();
			// 不是接口类型，则抛出异常
			if (!ics[i].isInterface()) {
				throw new RuntimeException(itf + " is not a interface.");
			}

			Class<?> tmp = null;
			try {
				// 反射重新加载接口类
				tmp = Class.forName(itf, false, cl);
			} catch (ClassNotFoundException e) {
			}
			// 检测接口是否相同，这里 tmp 有可能为空，不同则抛出异常
			if (tmp != ics[i]) {
				throw new IllegalArgumentException(ics[i] + " is not visible from class loader");
			}
			// 拼接接口全限定名，分隔符为 ;
			sb.append(itf).append(';');
		}

		// 使用拼接后的接口名作为 key
		String key = sb.toString();

		// 根据类加载器获取缓存
		final Map<String, Object> cache;
		synchronized (PROXY_CACHE_MAP) {
			cache = PROXY_CACHE_MAP.computeIfAbsent(cl, k -> new HashMap<>());
		}

		Proxy proxy = null;
		synchronized (cache) {
			do {
				// 从缓存中获取 Reference<Proxy> 实例，渠道且是Reference实例则强转返回
				Object value = cache.get(key);
				if (value instanceof Reference<?>) {
					proxy = (Proxy) ((Reference<?>) value).get();
					if (proxy != null) {
						return proxy;
					}
				}
				// 如果是挂起生成标记，则调用线程wait方法，保证只有一个线程操作
				if (value == PENDING_GENERATION_MARKER) {
					try {
						// 其他线程在此处进行等待
						cache.wait();
					} catch (InterruptedException e) {
					}
				} else {
					// 放置标志位到缓存中，并跳出 while 循环进行后续操作
					cache.put(key, PENDING_GENERATION_MARKER);
					break;
				}
			} while (true);
		}
		// 代理类计数器加1
		long id = PROXY_CLASS_COUNTER.getAndIncrement();
		String pkg = null;
		ClassGenerator ccp = null, ccm = null;
		try {
			// 创建 ClassGenerator 对象
			ccp = ClassGenerator.newInstance(cl);

			Set<String> worked = new HashSet<>();
			List<Method> methods = new ArrayList<>();

			for (int i = 0; i < ics.length; i++) {
				// 检测接口访问级别是否为 protected 或 privete
				if (!Modifier.isPublic(ics[i].getModifiers())) {
					// 获取接口包名
					String npkg = ics[i].getPackage().getName();
					if (pkg == null) {
						pkg = npkg;
					} else {
						// 非 public 级别的接口必须在同一个包下，否者抛出异常
						if (!pkg.equals(npkg)) {
							throw new IllegalArgumentException("non-public interfaces from different packages");
						}
					}
				}
				// 添加接口到 ClassGenerator 中
				ccp.addInterface(ics[i]);
				// 遍历接口方法
				for (Method method : ics[i].getMethods()) {
					// 获取方法描述，可理解为方法签名
					String desc = ReflectUtils.getDesc(method);
					// 如果方法描述字符串已在 worked 中或者是静态方法，则忽略。
					if (worked.contains(desc) || Modifier.isStatic(method.getModifiers())) {
						continue;
					}
					// 如果是接口并且是静态方法，则忽略
					if (ics[i].isInterface() && Modifier.isStatic(method.getModifiers())) {
						continue;
					}
					// 将签名存入worked中
					worked.add(desc);
					// 方法个数
					int ix = methods.size();
					// 方法返回值类型
					Class<?> rt = method.getReturnType();
					// 方法参数类型
					Class<?>[] pts = method.getParameterTypes();
					// 生成 Object[] args = new Object[pts.length]
					StringBuilder code = new StringBuilder("Object[] args = new Object[").append(pts.length)
							.append("];");
					for (int j = 0; j < pts.length; j++) {
						// 生成 args[0] =($W)$1;
						// 生成 args[1] =($W)$2;
						// ......
						code.append(" args[").append(j).append("] = ($w)$").append(j + 1).append(";");
					}
					// 生成 InvokerHandler 接口的 invoker 方法调用语句，如下：
					// Object ret = handler.invoke(this, methods[ix], args);
					code.append(" Object ret = handler.invoke(this, methods[").append(ix).append("], args);");
					if (!Void.TYPE.equals(rt)) {// 返回值类型不是void
						// 生成返回语句，形如 return (java.lang.String) ret;
						code.append(" return ").append(asArgument(rt, "ret")).append(";");
					}

					methods.add(method);
					// 添加方法名、访问控制符、参数列表、方法代码等信息到 ClassGenerator 中
					ccp.addMethod(method.getName(), method.getModifiers(), rt, pts, method.getExceptionTypes(),
							code.toString());
				}
			}
			// 包名为空，设置为Proxy的包名
			if (pkg == null) {
				pkg = PACKAGE_NAME;
			}

			// 构建接口代理类名称：pkg + ".proxy" + id，比如 org.apache.dubbo.proxy0
			String pcn = pkg + ".proxy" + id;
			ccp.setClassName(pcn);
			ccp.addField("public static java.lang.reflect.Method[] methods;");
			// 生成 private java.lang.reflect.InvocationHandler handler;
			ccp.addField("private " + InvocationHandler.class.getName() + " handler;");
			// 生成构造函数
			// porxy0(java.lang.reflect.InvocationHandler arg0) {
			// handler=$1;
			// }
			ccp.addConstructor(Modifier.PUBLIC, new Class<?>[] { InvocationHandler.class }, new Class<?>[0],
					"handler=$1;");
			// 添加无参构造
			ccp.addDefaultConstructor();
			// 生成接口代理类
			Class<?> clazz = ccp.toClass();
			clazz.getField("methods").set(null, methods.toArray(new Method[0]));

			// 构建 Proxy 子类名称，比如 Proxy1 等
			String fcn = Proxy.class.getName() + id;
			// 创建cl类实例
			ccm = ClassGenerator.newInstance(cl);
			// 设置实例名称
			ccm.setClassName(fcn);
			// 添加无参构造
			ccm.addDefaultConstructor();
			// 设置父类
			ccm.setSuperClass(Proxy.class);
			// 生成获取实例方法
			// public Object newInstance(java.lang.reflect.InvocationHandler h) {
			// return new org.apache.dubbo.proxy0($1);
			// }
			ccm.addMethod("public Object newInstance(" + InvocationHandler.class.getName() + " h){ return new " + pcn
					+ "($1); }");
			// 生成 Proxy 实现类
			Class<?> pc = ccm.toClass();
			// 通过反射创建 Proxy 实现类实例
			proxy = (Proxy) pc.newInstance();
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			// 释放类生成器资源 ClassGenerator
			if (ccp != null) {
				ccp.release();
			}
			if (ccm != null) {
				ccm.release();
			}
			synchronized (cache) {
				// 代理为空即生成失败，移除缓存
				if (proxy == null) {
					cache.remove(key);
				} else {
					// 生成代理成功，虚引用存入缓存
					cache.put(key, new WeakReference<Proxy>(proxy));
				}
				// 通知等待线程
				cache.notifyAll();
			}
		}
		return proxy;
	}

	private static String asArgument(Class<?> cl, String name) {
		if (cl.isPrimitive()) {
			if (Boolean.TYPE == cl) {
				return name + "==null?false:((Boolean)" + name + ").booleanValue()";
			}
			if (Byte.TYPE == cl) {
				return name + "==null?(byte)0:((Byte)" + name + ").byteValue()";
			}
			if (Character.TYPE == cl) {
				return name + "==null?(char)0:((Character)" + name + ").charValue()";
			}
			if (Double.TYPE == cl) {
				return name + "==null?(double)0:((Double)" + name + ").doubleValue()";
			}
			if (Float.TYPE == cl) {
				return name + "==null?(float)0:((Float)" + name + ").floatValue()";
			}
			if (Integer.TYPE == cl) {
				return name + "==null?(int)0:((Integer)" + name + ").intValue()";
			}
			if (Long.TYPE == cl) {
				return name + "==null?(long)0:((Long)" + name + ").longValue()";
			}
			if (Short.TYPE == cl) {
				return name + "==null?(short)0:((Short)" + name + ").shortValue()";
			}
			throw new RuntimeException(name + " is unknown primitive type.");
		}
		return "(" + ReflectUtils.getName(cl) + ")" + name;
	}

	/**
	 * get instance with default handler.
	 *
	 * @return instance.
	 */
	public Object newInstance() {
		return newInstance(THROW_UNSUPPORTED_INVOKER);
	}

	/**
	 * get instance with special handler.
	 *
	 * @return instance.
	 */
	abstract public Object newInstance(InvocationHandler handler);
}
