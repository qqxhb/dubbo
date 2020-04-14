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
package org.apache.dubbo.config;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.config.event.ReferenceConfigDestroyedEvent;
import org.apache.dubbo.config.event.ReferenceConfigInitializedEvent;
import org.apache.dubbo.config.utils.ConfigValidationUtils;
import org.apache.dubbo.event.Event;
import org.apache.dubbo.event.EventDispatcher;
import org.apache.dubbo.metadata.WritableMetadataService;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.cluster.support.registry.ZoneAwareCluster;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.AsyncMethodInfo;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.model.ServiceRepository;
import org.apache.dubbo.rpc.protocol.injvm.InjvmProtocol;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.CLUSTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.LOCALHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.METADATA_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.METHODS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.MONITOR_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROXY_CLASS_REF;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.CommonConstants.REVISION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SEMICOLON_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.utils.NetUtils.isInvalidLocalHost;
import static org.apache.dubbo.config.Constants.DUBBO_IP_TO_REGISTRY;
import static org.apache.dubbo.registry.Constants.CONSUMER_PROTOCOL;
import static org.apache.dubbo.registry.Constants.REGISTER_IP_KEY;
import static org.apache.dubbo.rpc.Constants.LOCAL_PROTOCOL;
import static org.apache.dubbo.rpc.cluster.Constants.REFER_KEY;

/**
 * Please avoid using this class for any new application, use
 * {@link ReferenceConfigBase} instead.
 */
public class ReferenceConfig<T> extends ReferenceConfigBase<T> {

	public static final Logger logger = LoggerFactory.getLogger(ReferenceConfig.class);

	/**
	 * The {@link Protocol} implementation with adaptive functionality,it will be
	 * different in different scenarios. A particular {@link Protocol}
	 * implementation is determined by the protocol attribute in the {@link URL}.
	 * For example:
	 *
	 * <li>when the url is
	 * registry://224.5.6.7:1234/org.apache.dubbo.registry.RegistryService?application=dubbo-sample,
	 * then the protocol is <b>RegistryProtocol</b></li>
	 *
	 * <li>when the url is
	 * dubbo://224.5.6.7:1234/org.apache.dubbo.config.api.DemoService?application=dubbo-sample,
	 * then the protocol is <b>DubboProtocol</b></li>
	 * <p>
	 * Actually，when the {@link ExtensionLoader} init the {@link Protocol}
	 * instants,it will automatically wraps two layers, and eventually will get a
	 * <b>ProtocolFilterWrapper</b> or <b>ProtocolListenerWrapper</b>
	 */
	private static final Protocol REF_PROTOCOL = ExtensionLoader.getExtensionLoader(Protocol.class)
			.getAdaptiveExtension();

	/**
	 * The {@link Cluster}'s implementation with adaptive functionality, and
	 * actually it will get a {@link Cluster}'s specific implementation who is
	 * wrapped with <b>MockClusterInvoker</b>
	 */
	private static final Cluster CLUSTER = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

	/**
	 * A {@link ProxyFactory} implementation that will generate a reference
	 * service's proxy,the JavassistProxyFactory is its default implementation
	 */
	private static final ProxyFactory PROXY_FACTORY = ExtensionLoader.getExtensionLoader(ProxyFactory.class)
			.getAdaptiveExtension();

	/**
	 * The interface proxy reference
	 */
	private transient volatile T ref;

	/**
	 * The invoker of the reference service
	 */
	private transient volatile Invoker<?> invoker;

	/**
	 * The flag whether the ReferenceConfig has been initialized
	 */
	private transient volatile boolean initialized;

	/**
	 * whether this ReferenceConfig has been destroyed
	 */
	private transient volatile boolean destroyed;

	private final ServiceRepository repository;

	private DubboBootstrap bootstrap;

	public ReferenceConfig() {
		super();
		this.repository = ApplicationModel.getServiceRepository();
	}

	public ReferenceConfig(Reference reference) {
		super(reference);
		this.repository = ApplicationModel.getServiceRepository();
	}

	public synchronized T get() {
		// 已经调用过销毁方法，则抛出异常
		if (destroyed) {
			throw new IllegalStateException("The invoker of ReferenceConfig(" + url + ") has already destroyed!");
		}
		// 检测 ref 是否为空，为空则通过 init 方法创建（要用于处理配置，以及调用 createProxy 生成代理类）
		if (ref == null) {
			init();
		}
		return ref;
	}

	public synchronized void destroy() {
		if (ref == null) {
			return;
		}
		if (destroyed) {
			return;
		}
		destroyed = true;
		try {
			invoker.destroy();
		} catch (Throwable t) {
			logger.warn("Unexpected error occured when destroy invoker of ReferenceConfig(" + url + ").", t);
		}
		invoker = null;
		ref = null;

		// dispatch a ReferenceConfigDestroyedEvent since 2.7.4
		dispatch(new ReferenceConfigDestroyedEvent(this));
	}

	public synchronized void init() {
		// 已经初始化则返回，避免重复操作
		if (initialized) {
			return;
		}
		// 如果DubboBootstrap为空，则构建DubboBootstrap实例并初始化
		// 这部分逻辑在超详细Dubbo服务导出源码解读的2.2已经详细介绍过
		// 主要是通过SPI方式获取各种配置管理实例，然后调用初始化方法，初始化配置、监听器等
		if (bootstrap == null) {
			bootstrap = DubboBootstrap.getInstance();
			bootstrap.init();
		}
		// 检查每个配置模块是否正确创建，并在必要时重写其属性。
		checkAndUpdateSubConfigs();
		// 校验本地存根合法性
		checkStubAndLocal(interfaceClass);
		// 校验mock配置合法性
		ConfigValidationUtils.checkMock(interfaceClass, this);
		// 添加配置<side,consumer>
		Map<String, String> map = new HashMap<String, String>();
		map.put(SIDE_KEY, CONSUMER_SIDE);
		// 添加版本、时间戳等运行时参数
		ReferenceConfigBase.appendRuntimeParameters(map);
		// 不是泛化服务(泛化就是服务消费者并没有服务的接口)
		if (!ProtocolUtils.isGeneric(generic)) {
			// 获取版本,校验存入map
			String revision = Version.getVersion(interfaceClass, version);
			if (revision != null && revision.length() > 0) {
				map.put(REVISION_KEY, revision);
			}
			// 获取包装类所有方法名称，放入map中，如果没有则放入<methods,*>
			String[] methods = Wrapper.getWrapper(interfaceClass).getMethodNames();
			if (methods.length == 0) {
				logger.warn("No method found in service interface " + interfaceClass.getName());
				map.put(METHODS_KEY, ANY_VALUE);
			} else {
				map.put(METHODS_KEY, StringUtils.join(new HashSet<String>(Arrays.asList(methods)), COMMA_SEPARATOR));
			}
		}
		// 接口放入map
		map.put(INTERFACE_KEY, interfaceName);
		// 通过getter或者getParameters方法将MetricsConfig、ApplicationConfig和ModuleConfig属性放到map中
		AbstractConfig.appendParameters(map, getMetrics());
		AbstractConfig.appendParameters(map, getApplication());
		AbstractConfig.appendParameters(map, getModule());
		// 通过getter或者getParameters方法将ConsumerConfig和ReferenceConfig属性放到map中
		AbstractConfig.appendParameters(map, consumer);
		AbstractConfig.appendParameters(map, this);
		// 元数据报告配置不为空且合法，则设置<metadata,remote>到map中
		MetadataReportConfig metadataReportConfig = getMetadataReportConfig();
		if (metadataReportConfig != null && metadataReportConfig.isValid()) {
			map.putIfAbsent(METADATA_KEY, REMOTE_METADATA_STORAGE_TYPE);
		}
		// 存放异步方法信息
		Map<String, AsyncMethodInfo> attributes = null;
		if (CollectionUtils.isNotEmpty(getMethods())) {
			attributes = new HashMap<>();
			// 遍历所有的方法配置存入map中
			for (MethodConfig methodConfig : getMethods()) {
				AbstractConfig.appendParameters(map, methodConfig, methodConfig.getName());
				String retryKey = methodConfig.getName() + ".retry";
				if (map.containsKey(retryKey)) {
					// 如果存在methodname.retry则移除
					String retryValue = map.remove(retryKey);
					// 如果retry对应的是false，则设置retries值为0
					if ("false".equals(retryValue)) {
						map.put(methodConfig.getName() + ".retries", "0");
					}
				}
				// 提取方法信息中的异步方法信息，存入map
				AsyncMethodInfo asyncMethodInfo = AbstractConfig.convertMethodConfig2AsyncInfo(methodConfig);
				if (asyncMethodInfo != null) {
//                    consumerModel.getMethodModel(methodConfig.getName()).addAttribute(ASYNC_KEY, asyncMethodInfo);
					attributes.put(methodConfig.getName(), asyncMethodInfo);
				}
			}
		}
		// 获取注册中心IP地址
		String hostToRegistry = ConfigUtils.getSystemProperty(DUBBO_IP_TO_REGISTRY);
		if (StringUtils.isEmpty(hostToRegistry)) {
			// 配置为空则取本机地址
			hostToRegistry = NetUtils.getLocalHost();
		} else if (isInvalidLocalHost(hostToRegistry)) {
			// 是无效的本地主机,则抛出异常
			throw new IllegalArgumentException("Specified invalid registry ip from property:" + DUBBO_IP_TO_REGISTRY
					+ ", value:" + hostToRegistry);
		}
		map.put(REGISTER_IP_KEY, hostToRegistry);
		// 将上面的配置信息添加到元数据附属信息中
		serviceMetadata.getAttachments().putAll(map);
		// 根据配置创建代理
		ref = createProxy(map);
		// 服务元数据中添加目标对象配置
		serviceMetadata.setTarget(ref);
		serviceMetadata.addAttribute(PROXY_CLASS_REF, ref);
		// 根据服务名，从缓存中获取 ConsumerModel，并将设置 ConsumerModel 代理对象为ref
		ConsumerModel consumerModel = repository.lookupReferredService(serviceMetadata.getServiceKey());
		consumerModel.setProxyObject(ref);
		// 将异步方法信息存入ConsumerModel并初始化
		consumerModel.init(attributes);
		// 设置初始化完成
		initialized = true;

		// 分发服务引入配置初始化事件 ReferenceConfigInitializedEvent since 2.7.4
		dispatch(new ReferenceConfigInitializedEvent(this, invoker));
	}

	@SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
	private T createProxy(Map<String, String> map) {
		/*
		 * 是否是本地服务引用 ： 如果指定了injvm， ->  如果指定了一个url，那么认为它是一个远程调用 ->  否则，请检查范围参数  ->
		 * 如果未指定作用域，但目标服务是在同一个JVM中提供的，则希望进行本地调用（默认）
		 */
		if (shouldJvmRefer(map)) {
			// 生成本地引用 URL，协议为 injvm
			URL url = new URL(LOCAL_PROTOCOL, LOCALHOST_VALUE, 0, interfaceClass.getName()).addParameters(map);
			// 调用 refer 方法构建 InjvmInvoker 实例
			invoker = REF_PROTOCOL.refer(interfaceClass, url);
			if (logger.isInfoEnabled()) {
				logger.info("Using injvm service " + interfaceClass.getName());
			}
		} else {
			// 远程调用

			urls.clear();
			// url 不为空，表明用户可能想进行点对点调用
			if (url != null && url.length() > 0) {
				// 当需要配置多个 url 时，可用分号(;)进行分割，这里会进行切分
				String[] us = SEMICOLON_SPLIT_PATTERN.split(url);
				if (us != null && us.length > 0) {
					for (String u : us) {
						URL url = URL.valueOf(u);
						// url路径为空
						if (StringUtils.isEmpty(url.getPath())) {
							// 设置接口全限定名为 url 路径
							url = url.setPath(interfaceName);
						}
						// 检测 url 协议是否为 registry
						if (UrlUtils.isRegistry(url)) {
							// 若是，表明用户想使用指定的注册中心,将 map 转换为查询字符串，并作为 refer 参数的值添加到 url 中
							urls.add(url.addParameterAndEncoded(REFER_KEY, StringUtils.toQueryString(map)));
						} else {
							// 合并 url，移除服务提供者的一些配置（这些配置来源于用户配置的 url 属性），
							// 比如线程池相关配置。并保留服务提供者的部分配置，比如版本，group，时间戳等
							// 最后将合并后的配置设置为 url 查询字符串中。
							urls.add(ClusterUtils.mergeUrl(url, map));
						}
					}
				}
			} else {
				// 如果协议不是injvm
				if (!LOCAL_PROTOCOL.equalsIgnoreCase(getProtocol())) {
					// 校验注册中心合法性
					checkRegistry();
					// 加载注册中心地址
					List<URL> us = ConfigValidationUtils.loadRegistries(this, false);
					if (CollectionUtils.isNotEmpty(us)) {
						for (URL u : us) {
							// 加载监控地址，不为空则放入map中
							URL monitorUrl = ConfigValidationUtils.loadMonitor(this, u);
							if (monitorUrl != null) {
								map.put(MONITOR_KEY, URL.encode(monitorUrl.toFullString()));
							}
							// 将 map 转换为查询字符串，并作为 refer 参数的值添加到 url 中
							urls.add(u.addParameterAndEncoded(REFER_KEY, StringUtils.toQueryString(map)));
						}
					}
					// 未配置注册中心，抛出异常
					if (urls.isEmpty()) {
						throw new IllegalStateException(
								"No such any registry to reference " + interfaceName + " on the consumer "
										+ NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion()
										+ ", please config <dubbo:registry address=\"...\" /> to your spring config.");
					}
				}
			}
			// 单个注册中心或服务提供者(服务直连，下同)
			if (urls.size() == 1) {
				// 调用 RegistryProtocol 的 refer 构建 Invoker 实例
				invoker = REF_PROTOCOL.refer(interfaceClass, urls.get(0));
			} else {
				// 多个注册中心或多个服务提供者，或者两者混合
				List<Invoker<?>> invokers = new ArrayList<Invoker<?>>();
				URL registryURL = null;
				// 遍历所有的注册中心或者服务提供者地址
				for (URL url : urls) {
					// 通过 refprotocol 调用 refer 构建 Invoker，refprotocol 会在运行时
					// 根据 url 协议头加载指定的 Protocol 实例，并调用实例的 refer 方法
					invokers.add(REF_PROTOCOL.refer(interfaceClass, url));
					if (UrlUtils.isRegistry(url)) {
						// 如果是注册中心地址，则将url赋值给注册中心
						registryURL = url;
					}
				}
				if (registryURL != null) {
					// 注册表url可用于多订阅方案，默认情况下使用“区域感知”策略（ZoneAwareCluster）
					URL u = registryURL.addParameterIfAbsent(CLUSTER_KEY, ZoneAwareCluster.NAME);
					// invoker包装层级如下：
					// ZoneAwareClusterInvoker(StaticDirectory) ->
					// FailoverClusterInvoker(RegistryDirectory, routing happens here) -> Invoker
					invoker = CLUSTER.join(new StaticDirectory(u, invokers));
				} else { // 没有注册中心地址则是直连，之间创建StaticDirectory
					invoker = CLUSTER.join(new StaticDirectory(invokers));
				}
			}
		}
		// 需要进行可用性检查但是Invoker可用
		if (shouldCheck() && !invoker.isAvailable()) {
			// invoker 执行销毁逻辑，并抛出异常
			invoker.destroy();
			throw new IllegalStateException("Failed to check the status of the service " + interfaceName
					+ ". No provider available for the service " + (group == null ? "" : group + "/") + interfaceName
					+ (version == null ? "" : ":" + version) + " from the url " + invoker.getUrl() + " to the consumer "
					+ NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion());
		}
		if (logger.isInfoEnabled()) {
			logger.info("Refer dubbo service " + interfaceClass.getName() + " from url " + invoker.getUrl());
		}
		/**
		 * @since 2.7.0 ServiceData Store 服务元数据存储，默认本地
		 */
		String metadata = map.get(METADATA_KEY);
		WritableMetadataService metadataService = WritableMetadataService
				.getExtension(metadata == null ? DEFAULT_METADATA_STORAGE_TYPE : metadata);
		if (metadataService != null) {
			URL consumerURL = new URL(CONSUMER_PROTOCOL, map.remove(REGISTER_IP_KEY), 0, map.get(INTERFACE_KEY), map);
			metadataService.publishServiceDefinition(consumerURL);
		}
		// 通过代理工厂创建代理
		return (T) PROXY_FACTORY.getProxy(invoker, ProtocolUtils.isGeneric(generic));
	}

	/**
	 * This method should be called right after the creation of this class's
	 * instance, before any property in other config modules is used. Check each
	 * config modules are created properly and override their properties if
	 * necessary.
	 */
	public void checkAndUpdateSubConfigs() {
		if (StringUtils.isEmpty(interfaceName)) {
			throw new IllegalStateException("<dubbo:reference interface=\"\" /> interface not allow null!");
		}
		completeCompoundConfigs(consumer);
		if (consumer != null) {
			if (StringUtils.isEmpty(registryIds)) {
				setRegistryIds(consumer.getRegistryIds());
			}
		}
		// get consumer's global configuration
		checkDefault();
		this.refresh();
		if (getGeneric() == null && getConsumer() != null) {
			setGeneric(getConsumer().getGeneric());
		}
		if (ProtocolUtils.isGeneric(generic)) {
			interfaceClass = GenericService.class;
		} else {
			try {
				interfaceClass = Class.forName(interfaceName, true, Thread.currentThread().getContextClassLoader());
			} catch (ClassNotFoundException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
			checkInterfaceAndMethods(interfaceClass, getMethods());
		}

		// init serivceMetadata
		serviceMetadata.setVersion(version);
		serviceMetadata.setGroup(group);
		serviceMetadata.setDefaultGroup(group);
		serviceMetadata.setServiceType(getActualInterface());
		serviceMetadata.setServiceInterfaceName(interfaceName);
		// TODO, uncomment this line once service key is unified
		serviceMetadata.setServiceKey(URL.buildKey(interfaceName, group, version));

		ServiceRepository repository = ApplicationModel.getServiceRepository();
		ServiceDescriptor serviceDescriptor = repository.registerService(interfaceClass);
		repository.registerConsumer(serviceMetadata.getServiceKey(), serviceDescriptor, this, null, serviceMetadata);

		resolveFile();
		ConfigValidationUtils.validateReferenceConfig(this);
		postProcessConfig();
	}

	/**
	 * Figure out should refer the service in the same JVM from configurations. The
	 * default behavior is true 1. if injvm is specified, then use it 2. then if a
	 * url is specified, then assume it's a remote call 3. otherwise, check scope
	 * parameter 4. if scope is not specified but the target service is provided in
	 * the same JVM, then prefer to make the local call, which is the default
	 * behavior
	 */
	protected boolean shouldJvmRefer(Map<String, String> map) {
		URL tmpUrl = new URL("temp", "localhost", 0, map);
		boolean isJvmRefer;
		if (isInjvm() == null) {
			// if a url is specified, don't do local reference
			if (url != null && url.length() > 0) {
				isJvmRefer = false;
			} else {
				// by default, reference local service if there is
				isJvmRefer = InjvmProtocol.getInjvmProtocol().isInjvmRefer(tmpUrl);
			}
		} else {
			isJvmRefer = isInjvm();
		}
		return isJvmRefer;
	}

	/**
	 * Dispatch an {@link Event event}
	 *
	 * @param event an {@link Event event}
	 * @since 2.7.5
	 */
	protected void dispatch(Event event) {
		EventDispatcher.getDefaultExtension().dispatch(event);
	}

	public DubboBootstrap getBootstrap() {
		return bootstrap;
	}

	public void setBootstrap(DubboBootstrap bootstrap) {
		this.bootstrap = bootstrap;
	}

	@SuppressWarnings("unused")
	private final Object finalizerGuardian = new Object() {
		@Override
		protected void finalize() throws Throwable {
			super.finalize();

			if (!ReferenceConfig.this.destroyed) {
				logger.warn("ReferenceConfig(" + url + ") is not DESTROYED when FINALIZE");

				/*
				 * don't destroy for now try { ReferenceConfig.this.destroy(); } catch
				 * (Throwable t) {
				 * logger.warn("Unexpected err when destroy invoker of ReferenceConfig(" + url +
				 * ") in finalize method!", t); }
				 */
			}
		}
	};

	private void postProcessConfig() {
		List<ConfigPostProcessor> configPostProcessors = ExtensionLoader.getExtensionLoader(ConfigPostProcessor.class)
				.getActivateExtension(URL.valueOf("configPostProcessor://"), (String[]) null);
		configPostProcessors.forEach(component -> component.postProcessReferConfig(this));
	}

	// just for test
	Invoker<?> getInvoker() {
		return invoker;
	}
}
