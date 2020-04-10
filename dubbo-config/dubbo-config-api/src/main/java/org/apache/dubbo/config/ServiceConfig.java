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
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.annotation.Service;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.config.event.ServiceConfigExportedEvent;
import org.apache.dubbo.config.event.ServiceConfigUnexportedEvent;
import org.apache.dubbo.config.invoker.DelegateProviderMetaDataInvoker;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.config.utils.ConfigValidationUtils;
import org.apache.dubbo.event.Event;
import org.apache.dubbo.event.EventDispatcher;
import org.apache.dubbo.metadata.WritableMetadataService;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.ConfiguratorFactory;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.model.ServiceRepository;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_IP_TO_BIND;
import static org.apache.dubbo.common.constants.CommonConstants.LOCALHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.METADATA_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.METHODS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.MONITOR_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.REGISTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.CommonConstants.REVISION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.utils.NetUtils.getAvailablePort;
import static org.apache.dubbo.common.utils.NetUtils.getLocalHost;
import static org.apache.dubbo.common.utils.NetUtils.isInvalidLocalHost;
import static org.apache.dubbo.common.utils.NetUtils.isInvalidPort;
import static org.apache.dubbo.config.Constants.DUBBO_IP_TO_REGISTRY;
import static org.apache.dubbo.config.Constants.DUBBO_PORT_TO_BIND;
import static org.apache.dubbo.config.Constants.DUBBO_PORT_TO_REGISTRY;
import static org.apache.dubbo.config.Constants.MULTICAST;
import static org.apache.dubbo.config.Constants.SCOPE_NONE;
import static org.apache.dubbo.remoting.Constants.BIND_IP_KEY;
import static org.apache.dubbo.remoting.Constants.BIND_PORT_KEY;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;
import static org.apache.dubbo.rpc.Constants.LOCAL_PROTOCOL;
import static org.apache.dubbo.rpc.Constants.PROXY_KEY;
import static org.apache.dubbo.rpc.Constants.SCOPE_KEY;
import static org.apache.dubbo.rpc.Constants.SCOPE_LOCAL;
import static org.apache.dubbo.rpc.Constants.SCOPE_REMOTE;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.EXPORT_KEY;

public class ServiceConfig<T> extends ServiceConfigBase<T> {

	public static final Logger logger = LoggerFactory.getLogger(ServiceConfig.class);

	/**
	 * A random port cache, the different protocols who has no port specified have
	 * different random port
	 */
	private static final Map<String, Integer> RANDOM_PORT_MAP = new HashMap<String, Integer>();

	/**
	 * A delayed exposure service timer
	 */
	private static final ScheduledExecutorService DELAY_EXPORT_EXECUTOR = Executors
			.newSingleThreadScheduledExecutor(new NamedThreadFactory("DubboServiceDelayExporter", true));

	private static final Protocol PROTOCOL = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();

	/**
	 * A {@link ProxyFactory} implementation that will generate a exported service
	 * proxy,the JavassistProxyFactory is its default implementation
	 */
	private static final ProxyFactory PROXY_FACTORY = ExtensionLoader.getExtensionLoader(ProxyFactory.class)
			.getAdaptiveExtension();

	/**
	 * Whether the provider has been exported
	 */
	private transient volatile boolean exported;

	/**
	 * The flag whether a service has unexported ,if the method unexported is
	 * invoked, the value is true
	 */
	private transient volatile boolean unexported;

	private DubboBootstrap bootstrap;

	/**
	 * The exported services
	 */
	private final List<Exporter<?>> exporters = new ArrayList<Exporter<?>>();

	public ServiceConfig() {
	}

	public ServiceConfig(Service service) {
		super(service);
	}

	@Parameter(excluded = true)
	public boolean isExported() {
		return exported;
	}

	@Parameter(excluded = true)
	public boolean isUnexported() {
		return unexported;
	}

	public void unexport() {
		if (!exported) {
			return;
		}
		if (unexported) {
			return;
		}
		if (!exporters.isEmpty()) {
			for (Exporter<?> exporter : exporters) {
				try {
					exporter.unexport();
				} catch (Throwable t) {
					logger.warn("Unexpected error occured when unexport " + exporter, t);
				}
			}
			exporters.clear();
		}
		unexported = true;

		// dispatch a ServiceConfigUnExportedEvent since 2.7.4
		dispatch(new ServiceConfigUnexportedEvent(this));
	}

	public synchronized void export() {
		// 判断当前服务是否需要导出
		if (!shouldExport()) {
			return;
		}
		// 启动类为空则获取一个实例
		if (bootstrap == null) {
			bootstrap = DubboBootstrap.getInstance();
			bootstrap.init();
		}
		// 校验并更细配置（默认配置、协议配置等）
		checkAndUpdateSubConfigs();

		// 初始化元数据（设置版本、分组、类型及名称等属性）
		serviceMetadata.setVersion(version);
		serviceMetadata.setGroup(group);
		serviceMetadata.setDefaultGroup(group);
		serviceMetadata.setServiceType(getInterfaceClass());
		serviceMetadata.setServiceInterfaceName(getInterface());
		serviceMetadata.setTarget(getRef());
		// 是否需要延迟导出
		if (shouldDelay()) {
			// 提交导出任务到延迟导出调度器（可调度线程池）
			DELAY_EXPORT_EXECUTOR.schedule(this::doExport, getDelay(), TimeUnit.MILLISECONDS);
		} else {
			// 执行导出操作
			doExport();
		}
		// 执行已导出操作
		exported();
	}

	public void exported() {
		// 发布一个服务导出事件（ ServiceConfigExportedEvent）
		dispatch(new ServiceConfigExportedEvent(this));
	}

	private void checkAndUpdateSubConfigs() {
		// 完成组件注册（协议、注册中心）
		completeCompoundConfigs();
		// 检查provider是否为空，为空则获取默认默认的provider，默认的不存在则新建一个ProviderConfig实例
		checkDefault();
		// 检查协议，如果协议为空并且provider不为空，则获取provider中的协议；将根据协议ID转换协议
		checkProtocol();
		// 初始化一些为空的配置
		List<ConfigInitializer> configInitializers = ExtensionLoader.getExtensionLoader(ConfigInitializer.class)
				.getActivateExtension(URL.valueOf("configInitializer://"), (String[]) null);
		configInitializers.forEach(e -> e.initServiceConfig(this));

		// 如果协议不是仅仅是injvm即需要导出服务给外部使用，则校验注册中心
		if (!isOnlyInJvm()) {
			// 检验注册中心配置是否存在，然后通过id转换成注册中心配置RegistryConfig
			// 转换完成后遍历所有Registry是否可用（注册地址是否为空），不可用则会抛出异常
			checkRegistry();
		}
		// 调用刷新配置方法
		// 1.
		// 首先调用org.apache.dubbo.common.config.Environment.getPrefixedConfiguration(AbstractConfig)方法，
		// 该方法会从多种配置源（AbstractConfig (API, XML, annotation), - D, config
		// center）中找出Application, Registry, Protocol等的最优配置
		// 2.通过getClass().getMethods()获取所有的方法，判断是否是setter或者setParameters方法，如果是就通过反射调用将将新的配置设置进去
		this.refresh();
		// 接口名称为空，抛出异常
		if (StringUtils.isEmpty(interfaceName)) {
			throw new IllegalStateException("<dubbo:service interface=\"\" /> interface not allow null!");
		}
		// 检查引用实现是否是泛化服务类型
		if (ref instanceof GenericService) {
			// 设置接口类为泛化服务类
			interfaceClass = GenericService.class;
			if (StringUtils.isEmpty(generic)) {
				// 泛化服务标识符为空则设置为true
				generic = Boolean.TRUE.toString();
			}
		} else {// 不是泛化服务类型
			try {
				// 接口类通过接口名加载
				interfaceClass = Class.forName(interfaceName, true, Thread.currentThread().getContextClassLoader());
			} catch (ClassNotFoundException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
			// 检查远程服务接口和方法是否符合Dubbo的要求，主要检查配置文件中配置的方法是否包含在远程服务接口中
			checkInterfaceAndMethods(interfaceClass, getMethods());
			// 检查引用不应为空，并且是给定接口的实现
			checkRef();
			generic = Boolean.FALSE.toString();
		}
		// 服务接口的本地实现类名不为空
		if (local != null) {
			// 如果local不是类名而是true，则拼接类名
			if ("true".equals(local)) {
				local = interfaceName + "Local";
			}
			Class<?> localClass;
			try {
				// 根据名称使用线程上下文类加载器加载本地类
				localClass = ClassUtils.forNameWithThreadContextClassLoader(local);
			} catch (ClassNotFoundException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
			// 如果本地类没有实现指定接口，则抛出异常
			if (!interfaceClass.isAssignableFrom(localClass)) {
				throw new IllegalStateException("The local implementation class " + localClass.getName()
						+ " not implement interface " + interfaceName);
			}
		}
		// 本地存根和上面的local逻辑一致
		if (stub != null) {
			if ("true".equals(stub)) {
				stub = interfaceName + "Stub";
			}
			Class<?> stubClass;
			try {
				stubClass = ClassUtils.forNameWithThreadContextClassLoader(stub);
			} catch (ClassNotFoundException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
			if (!interfaceClass.isAssignableFrom(stubClass)) {
				throw new IllegalStateException("The stub implementation class " + stubClass.getName()
						+ " not implement interface " + interfaceName);
			}
		}
		// 本地存根合法性校验，主要也是校验了是否是之指定接口的实现
		checkStubAndLocal(interfaceClass);
		// Mock校验，主要校验值mock值是否合法及mock类是否合法（实现指定接口并且有默认构造函数）
		ConfigValidationUtils.checkMock(interfaceClass, this);
		// 服务配置校验（版本路径票据、扩展校验（ExporterListener、ProxyFactory、InvokerListener、Cluster等）、注册中心、协议即提供者校验）
		ConfigValidationUtils.validateServiceConfig(this);
		// 配置后置处理器调用ConfigPostProcessor#postProcessServiceConfig方法
		postProcessConfig();
	}

	protected synchronized void doExport() {
		// 是否执行了unexport方法（表示服务注销了），执行了则抛出异常
		if (unexported) {
			throw new IllegalStateException("The service " + interfaceClass.getName() + " has already unexported!");
		}
		// 已经导出过则直接返回
		if (exported) {
			return;
		}
		// 设置已导出标志
		exported = true;
		// 路径及服务名为空
		if (StringUtils.isEmpty(path)) {
			// 服务名为接口名
			path = interfaceName;
		}
		// 执行多协议导出
		doExportUrls();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void doExportUrls() {
		// 获取服务缓存库
		ServiceRepository repository = ApplicationModel.getServiceRepository();
		// 注册当前服务到本地缓存库
		ServiceDescriptor serviceDescriptor = repository.registerService(getInterfaceClass());
		// 注册服务提供者到缓存库
		repository.registerProvider(
				// 根据接口名，服务组及版本号生成唯一名称
				getUniqueServiceName(), ref, serviceDescriptor, this, serviceMetadata);
		// 加载注册中心地址（支持多注册中心，因此是集合）
		List<URL> registryURLs = ConfigValidationUtils.loadRegistries(this, true);

		for (ProtocolConfig protocolConfig : protocols) {// 支持多协议导出
			// 根据协议配置生成服务地址（注册中心的key）
			String pathKey = URL.buildKey(getContextPath(protocolConfig).map(p -> p + "/" + path).orElse(path), group,
					version);
			// 如果指定了服务路径，需要再次注册到缓存中，保证此映射路径能获取到服务
			repository.registerService(pathKey, interfaceClass);
			// 设置原数据服务key
			serviceMetadata.setServiceKey(pathKey);
			// 执行多中心单协议的导出逻辑
			doExportUrlsFor1Protocol(protocolConfig, registryURLs);
		}
	}

	private void doExportUrlsFor1Protocol(ProtocolConfig protocolConfig, List<URL> registryURLs) {
		String name = protocolConfig.getName();
		// 协议名称为空，设置为dubbo
		if (StringUtils.isEmpty(name)) {
			name = DUBBO;
		}
		// 添加配置<side,provider>
		Map<String, String> map = new HashMap<String, String>();
		map.put(SIDE_KEY, PROVIDER_SIDE);
		// 添加dubbo、版本、时间戳等运行参数到map中
		ServiceConfig.appendRuntimeParameters(map);
		// 通过getter或者getParameters方法将ApplicationConfig、ModuleConfig、MetricsConfig、
		// ProviderConfig、ProtocolConfig及ServiceConfig属性放到map中
		AbstractConfig.appendParameters(map, getMetrics());
		AbstractConfig.appendParameters(map, getApplication());
		AbstractConfig.appendParameters(map, provider);
		AbstractConfig.appendParameters(map, protocolConfig);
		AbstractConfig.appendParameters(map, this);
		// 获取原数导出配置，如果合法则添加<metadata,remote>到map中
		MetadataReportConfig metadataReportConfig = getMetadataReportConfig();
		if (metadataReportConfig != null && metadataReportConfig.isValid()) {
			map.putIfAbsent(METADATA_KEY, REMOTE_METADATA_STORAGE_TYPE);
		}
		//是否存在 <dubbo:method> 标签的配置信息
		if (CollectionUtils.isNotEmpty(getMethods())) {
			for (MethodConfig method : getMethods()) {
				// 通过getter或者getParameters方法将MethodConfig属性添加到map中，前缀是当前MethodConfig名称即<方法名.属性名,属性值>。
				AbstractConfig.appendParameters(map, method, method.getName());
				String retryKey = method.getName() + ".retry";
				//是否存在methodname.retry键，及是否配置了retry属性
				if (map.containsKey(retryKey)) {
					//存在则移除
					String retryValue = map.remove(retryKey);
					//如果retry配置的false，设置retries配置为0
					if ("false".equals(retryValue)) {
						map.put(method.getName() + ".retries", "0");
					}
				}
				//获取方法参数配置ArgumentConfig，存放到map中
				List<ArgumentConfig> arguments = method.getArguments();
				if (CollectionUtils.isNotEmpty(arguments)) {
					for (ArgumentConfig argument : arguments) {
						// 判断参数类型是否为空
						if (argument.getType() != null && argument.getType().length() > 0) {
							//获取接口（导出服务）的所有方法，遍历
							Method[] methods = interfaceClass.getMethods();
							if (methods.length > 0) {
								for (int i = 0; i < methods.length; i++) {
									String methodName = methods[i].getName();
									// 接口方法名和方法配置名称相同
									if (methodName.equals(method.getName())) {
										//获取接口方法参数类型
										Class<?>[] argtypes = methods[i].getParameterTypes();
										// 参数索引不是-1，-1表示未设置
										if (argument.getIndex() != -1) {
											 // 检测 ArgumentConfig 中的 type 属性与方法参数列表中的参数名称是否一致
											if (argtypes[argument.getIndex()].getName().equals(argument.getType())) {
												//一致的则添加配置到map<方法名.参数索引,参数配置>
												AbstractConfig.appendParameters(map, argument,
														method.getName() + "." + argument.getIndex());
											} else {//不一致则抛出异常
												throw new IllegalArgumentException(
														"Argument config error : the index attribute and type attribute not match :index :"
																+ argument.getIndex() + ", type:" + argument.getType());
											}
										} else {//未设置参数索引
											// 遍历方法参数的所有类型
											for (int j = 0; j < argtypes.length; j++) {
												Class<?> argclazz = argtypes[j];
												//查找和当前参数配置类型匹配的参数
												if (argclazz.getName().equals(argument.getType())) {
													//匹配则将配置放入map中<方法名.参数索引,参数配置>
													AbstractConfig.appendParameters(map, argument,
															method.getName() + "." + j);
													//如果匹配到的参数类型设置了索引并且和当前索引不一致，抛出异常
													if (argument.getIndex() != -1 && argument.getIndex() != j) {
														throw new IllegalArgumentException(
																"Argument config error : the index attribute and type attribute not match :index :"
																		+ argument.getIndex() + ", type:"
																		+ argument.getType());
													}
												}
											}
										}
									}
								}
							}
						} else if (argument.getIndex() != -1) {
							//参数类型为空但是参数索引不是-1，添加配置到map中
							AbstractConfig.appendParameters(map, argument,
									method.getName() + "." + argument.getIndex());
						} else {
							//如果既没有配置参数索引又没哟配置参数类型则抛出异常
							throw new IllegalArgumentException(
									"Argument config must set index or type attribute.eg: <dubbo:argument index='0' .../> or <dubbo:argument type=xxx .../>");
						}

					}
				}
			} 
		}
		
		if (ProtocolUtils.isGeneric(generic)) {
			//是泛化服务则设置<generic,generic值>、<methods,*>表示任意方法
			map.put(GENERIC_KEY, generic);
			map.put(METHODS_KEY, ANY_VALUE);
		} else {
			//不是泛化服务，获取修订版本号，放入map
			String revision = Version.getVersion(interfaceClass, version);
			if (revision != null && revision.length() > 0) {
				map.put(REVISION_KEY, revision);
			}
			//创建并返回包装类方法名称
			String[] methods = Wrapper.getWrapper(interfaceClass).getMethodNames();
			if (methods.length == 0) {
				//没有包装方法名则设置<methods,*>
				logger.warn("No method found in service interface " + interfaceClass.getName());
				map.put(METHODS_KEY, ANY_VALUE);
			} else {
				//如果有，则方面通过逗号分隔拼接放入map中，key=methods
				map.put(METHODS_KEY, StringUtils.join(new HashSet<String>(Arrays.asList(methods)), ","));
			}
		}

		//没有token且提供者不为空,获取提供者的token
		if (ConfigUtils.isEmpty(token) && provider != null) {
			token = provider.getToken();
		}
		//如果token不为空
		if (!ConfigUtils.isEmpty(token)) {
			//如果token是默认值（true或者default）,则创建UUID作为token
			if (ConfigUtils.isDefault(token)) {
				map.put(TOKEN_KEY, UUID.randomUUID().toString());
			} else {
				map.put(TOKEN_KEY, token);
			}
		}
		// init serviceMetadata attachments
		serviceMetadata.getAttachments().putAll(map);

		// 找到配置的主机，优先级如下：environment variables -> java system properties -> host property in config file 
		// -> /etc/hosts -> default network address -> first available network address
		String host = findConfigedHosts(protocolConfig, registryURLs, map);
		// 找到配置的端口，优先级： environment variable -> java system properties ->
		// port property in protocol config file -> protocol default port
		Integer port = findConfigedPorts(protocolConfig, name, map);
		//拼接URL
		URL url = new URL(name, host, port, getContextPath(protocolConfig).map(p -> p + "/" + path).orElse(path), map);

		// 如果存在则获取自定义扩展配置
		if (ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class).hasExtension(url.getProtocol())) {
			url = ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class).getExtension(url.getProtocol())
					.getConfigurator(url).configure(url);
		}
		//获取导出范围
		String scope = url.getParameter(SCOPE_KEY);
		// 如果导出单位不是none（none则不执行导出）
		if (!SCOPE_NONE.equalsIgnoreCase(scope)) {

			// 如果导出范围不是remote则执行本地导出逻辑
			if (!SCOPE_REMOTE.equalsIgnoreCase(scope)) {
				exportLocal(url);
			}
			// 如果配置导出范围不是local则执行远程导出
			if (!SCOPE_LOCAL.equalsIgnoreCase(scope)) {
				//注册中心地址不为空
				if (CollectionUtils.isNotEmpty(registryURLs)) {
					for (URL registryURL : registryURLs) {
						// 如果协议是injvm 则不执行导出注册逻辑
						if (LOCAL_PROTOCOL.equalsIgnoreCase(url.getProtocol())) {
							continue;
						}
						//url配置dynamic参数
						url = url.addParameterIfAbsent(DYNAMIC_KEY, registryURL.getParameter(DYNAMIC_KEY));
						//获取监控地址
						URL monitorUrl = ConfigValidationUtils.loadMonitor(this, registryURL);
						if (monitorUrl != null) {
							//监控地址不为空则添加到URL参数中key=monitor
							url = url.addParameterAndEncoded(MONITOR_KEY, monitorUrl.toFullString());
						}
						if (logger.isInfoEnabled()) {
							if (url.getParameter(REGISTER_KEY, true)) {
								logger.info("Register dubbo service " + interfaceClass.getName() + " url " + url
										+ " to registry " + registryURL);
							} else {
								logger.info("Export dubbo service " + interfaceClass.getName() + " to url " + url);
							}
						}

						// 获取自定义代理配置
						String proxy = url.getParameter(PROXY_KEY);
						if (StringUtils.isNotEmpty(proxy)) {
							//如果存在，则为注册地址添加代理实现参数
							registryURL = registryURL.addParameter(PROXY_KEY, proxy);
						}
						//为服务引用生成Invoker对象
						Invoker<?> invoker = PROXY_FACTORY.getInvoker(ref, (Class) interfaceClass,
								registryURL.addParameterAndEncoded(EXPORT_KEY, url.toFullString()));
						//生成提供者和配置包装Invoker
						DelegateProviderMetaDataInvoker wrapperInvoker = new DelegateProviderMetaDataInvoker(invoker,
								this);
						//通过SPI自适应拓展获取Protocol的拓展实现，调用导出方法
						Exporter<?> exporter = PROTOCOL.export(wrapperInvoker);
						//添加到导出器缓存
						exporters.add(exporter);
					}
				} else {
					//注册中心地址为空，导出服务到配置地址
					if (logger.isInfoEnabled()) {
						logger.info("Export dubbo service " + interfaceClass.getName() + " to url " + url);
					}
					Invoker<?> invoker = PROXY_FACTORY.getInvoker(ref, (Class) interfaceClass, url);
					DelegateProviderMetaDataInvoker wrapperInvoker = new DelegateProviderMetaDataInvoker(invoker, this);

					Exporter<?> exporter = PROTOCOL.export(wrapperInvoker);
					exporters.add(exporter);
				}
				/**
				 * @since 2.7.0 ServiceData Store
				 * 获取可写元数据服务，默认实现为本地
				 */
				WritableMetadataService metadataService = WritableMetadataService
						.getExtension(url.getParameter(METADATA_KEY, DEFAULT_METADATA_STORAGE_TYPE));
				if (metadataService != null) {
					//发布服务定义
					metadataService.publishServiceDefinition(url);
				}
			}
		}
		//添加到服务引用url缓存中
		this.urls.add(url);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	/**
	 * always export injvm
	 */
	private void exportLocal(URL url) {
		URL local = URLBuilder.from(url).setProtocol(LOCAL_PROTOCOL).setHost(LOCALHOST_VALUE).setPort(0).build();
		Exporter<?> exporter = PROTOCOL.export(PROXY_FACTORY.getInvoker(ref, (Class) interfaceClass, local));
		exporters.add(exporter);
		logger.info("Export dubbo service " + interfaceClass.getName() + " to local registry url : " + local);
	}

	/**
	 * Determine if it is injvm
	 *
	 * @return
	 */
	private boolean isOnlyInJvm() {
		return getProtocols().size() == 1 && LOCAL_PROTOCOL.equalsIgnoreCase(getProtocols().get(0).getName());
	}

	/**
	 * Register & bind IP address for service provider, can be configured
	 * separately. Configuration priority: environment variables -> java system
	 * properties -> host property in config file -> /etc/hosts -> default network
	 * address -> first available network address
	 *
	 * @param protocolConfig
	 * @param registryURLs
	 * @param map
	 * @return
	 */
	private String findConfigedHosts(ProtocolConfig protocolConfig, List<URL> registryURLs, Map<String, String> map) {
		boolean anyhost = false;

		String hostToBind = getValueFromConfig(protocolConfig, DUBBO_IP_TO_BIND);
		if (hostToBind != null && hostToBind.length() > 0 && isInvalidLocalHost(hostToBind)) {
			throw new IllegalArgumentException(
					"Specified invalid bind ip from property:" + DUBBO_IP_TO_BIND + ", value:" + hostToBind);
		}

		// if bind ip is not found in environment, keep looking up
		if (StringUtils.isEmpty(hostToBind)) {
			hostToBind = protocolConfig.getHost();
			if (provider != null && StringUtils.isEmpty(hostToBind)) {
				hostToBind = provider.getHost();
			}
			if (isInvalidLocalHost(hostToBind)) {
				anyhost = true;
				try {
					logger.info("No valid ip found from environment, try to find valid host from DNS.");
					hostToBind = InetAddress.getLocalHost().getHostAddress();
				} catch (UnknownHostException e) {
					logger.warn(e.getMessage(), e);
				}
				if (isInvalidLocalHost(hostToBind)) {
					if (CollectionUtils.isNotEmpty(registryURLs)) {
						for (URL registryURL : registryURLs) {
							if (MULTICAST.equalsIgnoreCase(registryURL.getParameter("registry"))) {
								// skip multicast registry since we cannot connect to it via Socket
								continue;
							}
							try (Socket socket = new Socket()) {
								SocketAddress addr = new InetSocketAddress(registryURL.getHost(),
										registryURL.getPort());
								socket.connect(addr, 1000);
								hostToBind = socket.getLocalAddress().getHostAddress();
								break;
							} catch (Exception e) {
								logger.warn(e.getMessage(), e);
							}
						}
					}
					if (isInvalidLocalHost(hostToBind)) {
						hostToBind = getLocalHost();
					}
				}
			}
		}

		map.put(BIND_IP_KEY, hostToBind);

		// registry ip is not used for bind ip by default
		String hostToRegistry = getValueFromConfig(protocolConfig, DUBBO_IP_TO_REGISTRY);
		if (hostToRegistry != null && hostToRegistry.length() > 0 && isInvalidLocalHost(hostToRegistry)) {
			throw new IllegalArgumentException("Specified invalid registry ip from property:" + DUBBO_IP_TO_REGISTRY
					+ ", value:" + hostToRegistry);
		} else if (StringUtils.isEmpty(hostToRegistry)) {
			// bind ip is used as registry ip by default
			hostToRegistry = hostToBind;
		}

		map.put(ANYHOST_KEY, String.valueOf(anyhost));

		return hostToRegistry;
	}

	/**
	 * Register port and bind port for the provider, can be configured separately
	 * Configuration priority: environment variable -> java system properties ->
	 * port property in protocol config file -> protocol default port
	 *
	 * @param protocolConfig
	 * @param name
	 * @return
	 */
	private Integer findConfigedPorts(ProtocolConfig protocolConfig, String name, Map<String, String> map) {
		Integer portToBind = null;

		// parse bind port from environment
		String port = getValueFromConfig(protocolConfig, DUBBO_PORT_TO_BIND);
		portToBind = parsePort(port);

		// if there's no bind port found from environment, keep looking up.
		if (portToBind == null) {
			portToBind = protocolConfig.getPort();
			if (provider != null && (portToBind == null || portToBind == 0)) {
				portToBind = provider.getPort();
			}
			final int defaultPort = ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(name)
					.getDefaultPort();
			if (portToBind == null || portToBind == 0) {
				portToBind = defaultPort;
			}
			if (portToBind <= 0) {
				portToBind = getRandomPort(name);
				if (portToBind == null || portToBind < 0) {
					portToBind = getAvailablePort(defaultPort);
					putRandomPort(name, portToBind);
				}
			}
		}

		// save bind port, used as url's key later
		map.put(BIND_PORT_KEY, String.valueOf(portToBind));

		// registry port, not used as bind port by default
		String portToRegistryStr = getValueFromConfig(protocolConfig, DUBBO_PORT_TO_REGISTRY);
		Integer portToRegistry = parsePort(portToRegistryStr);
		if (portToRegistry == null) {
			portToRegistry = portToBind;
		}

		return portToRegistry;
	}

	private Integer parsePort(String configPort) {
		Integer port = null;
		if (configPort != null && configPort.length() > 0) {
			try {
				Integer intPort = Integer.parseInt(configPort);
				if (isInvalidPort(intPort)) {
					throw new IllegalArgumentException("Specified invalid port from env value:" + configPort);
				}
				port = intPort;
			} catch (Exception e) {
				throw new IllegalArgumentException("Specified invalid port from env value:" + configPort);
			}
		}
		return port;
	}

	private String getValueFromConfig(ProtocolConfig protocolConfig, String key) {
		String protocolPrefix = protocolConfig.getName().toUpperCase() + "_";
		String value = ConfigUtils.getSystemProperty(protocolPrefix + key);
		if (StringUtils.isEmpty(value)) {
			value = ConfigUtils.getSystemProperty(key);
		}
		return value;
	}

	private Integer getRandomPort(String protocol) {
		protocol = protocol.toLowerCase();
		return RANDOM_PORT_MAP.getOrDefault(protocol, Integer.MIN_VALUE);
	}

	private void putRandomPort(String protocol, Integer port) {
		protocol = protocol.toLowerCase();
		if (!RANDOM_PORT_MAP.containsKey(protocol)) {
			RANDOM_PORT_MAP.put(protocol, port);
			logger.warn("Use random available port(" + port + ") for protocol " + protocol);
		}
	}

	private void postProcessConfig() {
		List<ConfigPostProcessor> configPostProcessors = ExtensionLoader.getExtensionLoader(ConfigPostProcessor.class)
				.getActivateExtension(URL.valueOf("configPostProcessor://"), (String[]) null);
		configPostProcessors.forEach(component -> component.postProcessServiceConfig(this));
	}

	/**
	 * Dispatch an {@link Event event}
	 *
	 * @param event an {@link Event event}
	 * @since 2.7.5
	 */
	private void dispatch(Event event) {
		EventDispatcher.getDefaultExtension().dispatch(event);
	}

	public DubboBootstrap getBootstrap() {
		return bootstrap;
	}

	public void setBootstrap(DubboBootstrap bootstrap) {
		this.bootstrap = bootstrap;
	}
}
