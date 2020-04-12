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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;

/**
 * Wrapper.
 */
public abstract class Wrapper {
	private static final Map<Class<?>, Wrapper> WRAPPER_MAP = new ConcurrentHashMap<Class<?>, Wrapper>(); // class
																											// wrapper
																											// map
	private static final String[] EMPTY_STRING_ARRAY = new String[0];
	private static final String[] OBJECT_METHODS = new String[] { "getClass", "hashCode", "toString", "equals" };
	private static final Wrapper OBJECT_WRAPPER = new Wrapper() {
		@Override
		public String[] getMethodNames() {
			return OBJECT_METHODS;
		}

		@Override
		public String[] getDeclaredMethodNames() {
			return OBJECT_METHODS;
		}

		@Override
		public String[] getPropertyNames() {
			return EMPTY_STRING_ARRAY;
		}

		@Override
		public Class<?> getPropertyType(String pn) {
			return null;
		}

		@Override
		public Object getPropertyValue(Object instance, String pn) throws NoSuchPropertyException {
			throw new NoSuchPropertyException("Property [" + pn + "] not found.");
		}

		@Override
		public void setPropertyValue(Object instance, String pn, Object pv) throws NoSuchPropertyException {
			throw new NoSuchPropertyException("Property [" + pn + "] not found.");
		}

		@Override
		public boolean hasProperty(String name) {
			return false;
		}

		@Override
		public Object invokeMethod(Object instance, String mn, Class<?>[] types, Object[] args)
				throws NoSuchMethodException {
			if ("getClass".equals(mn)) {
				return instance.getClass();
			}
			if ("hashCode".equals(mn)) {
				return instance.hashCode();
			}
			if ("toString".equals(mn)) {
				return instance.toString();
			}
			if ("equals".equals(mn)) {
				if (args.length == 1) {
					return instance.equals(args[0]);
				}
				throw new IllegalArgumentException("Invoke method [" + mn + "] argument number error.");
			}
			throw new NoSuchMethodException("Method [" + mn + "] not found.");
		}
	};
	private static AtomicLong WRAPPER_CLASS_COUNTER = new AtomicLong(0);

	/**
	 * get wrapper.
	 *
	 * @param c Class instance.
	 * @return Wrapper instance(not null).
	 */
	public static Wrapper getWrapper(Class<?> c) {
		// 因为无法代理动态类，因此是动态类时会循环找到不是动态类的父类
		while (ClassGenerator.isDynamicClass(c)) { // 获取父类
			c = c.getSuperclass();
		}
		// 如果代理Object类，则直接返回固定实现OBJECT_WRAPPER
		if (c == Object.class) {
			return OBJECT_WRAPPER;
		}
		// 代理其他类则调用makeWrapper创建一个实例，并存入缓存中（如果不存在），然后返回该包装类实例
		return WRAPPER_MAP.computeIfAbsent(c, key -> makeWrapper(key));
	}

	private static Wrapper makeWrapper(Class<?> c) {
		// 判断是否是基本类型的包装类，如果是则抛出异常
		if (c.isPrimitive()) {
			throw new IllegalArgumentException("Can not create wrapper for primitive type: " + c);
		}
		// 获取类的全限定名称(包名+类名)
		String name = c.getName();
		// 获取类加载器，优先级如下：线程上下文加载器 -> c的类加载器 -> 系统类加载器
		ClassLoader cl = ClassUtils.getClassLoader(c);
		// 存放setPropertyValue方法代码
		StringBuilder c1 = new StringBuilder("public void setPropertyValue(Object o, String n, Object v){ ");
		// 存放getPropertyValue方法代码
		StringBuilder c2 = new StringBuilder("public Object getPropertyValue(Object o, String n){ ");
		// 存放invokeMethod方法代码
		StringBuilder c3 = new StringBuilder(
				"public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws "
						+ InvocationTargetException.class.getName() + "{ ");

		// 生成类型转换代码及异常抛出代码
		c1.append(name).append(" w; try{ w = ((").append(name)
				.append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
		c2.append(name).append(" w; try{ w = ((").append(name)
				.append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
		c3.append(name).append(" w; try{ w = ((").append(name)
				.append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
		// 存放属性<属性名,属性类型>
		Map<String, Class<?>> pts = new HashMap<>();
		// 存放方法<描述信息（可理解为方法签名）, Method 实例>
		Map<String, Method> ms = new LinkedHashMap<>();
		// 存放所有的方法名
		List<String> mns = new ArrayList<>();
		// 所有在当前类中声明的方法名
		List<String> dmns = new ArrayList<>();

		// 获取 public访问级别的字段
		for (Field f : c.getFields()) {
			String fn = f.getName();
			Class<?> ft = f.getType();
			// 如果是static 或 transient 修饰的变量，则忽略
			if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers())) {
				continue;
			}
			// 生成条件判断及赋值语句，比如：
			// if( $2.equals("name") ) { w.name = (java.lang.String) $3; return;}
			c1.append(" if( $2.equals(\"").append(fn).append("\") ){ w.").append(fn).append("=").append(arg(ft, "$3"))
					.append("; return; }");
			// 生成条件判断及返回语句，比如：
			// if( $2.equals("name") ) { return ($w)w.name; }
			c2.append(" if( $2.equals(\"").append(fn).append("\") ){ return ($w)w.").append(fn).append("; }");
			// 添加<属性名,属性类型>到集合中
			pts.put(fn, ft);
		}
		
		//获取所有public访问级别的方法
		Method[] methods = c.getMethods();
		// 检测是否包含在当前类中声明的方法
		boolean hasMethod = hasMethods(methods);
		if (hasMethod) {
			c3.append(" try{");
			for (Method m : methods) {
				// 忽略Object类声明的方法
				if (m.getDeclaringClass() == Object.class) {
					continue;
				}

				String mn = m.getName();
				// 生成方法名判断语句，比如：
		        // if ( "setName".equals( $2 )
				c3.append(" if( \"").append(mn).append("\".equals( $2 ) ");
				//参数个数
				int len = m.getParameterTypes().length;
				// 生成“运行时传入的参数数量与方法参数列表长度”判断语句，比如：
		        // && $3.length == 1
				c3.append(" && ").append(" $3.length == ").append(len);
				 // 检测方法是否存在重载情况，条件为：方法对象不同 && 方法名相同
				boolean override = false;
				for (Method m2 : methods) {
					if (m != m2 && m.getName().equals(m2.getName())) {
						override = true;
						break;
					}
				}
				// 对重载方法进行处理。判断是统一个方法时除了方法名、参数列表长度相同外，还必须判断参数类型是否一致，如：
				// void setName(String name) 和  void setName(Integer name)
				if (override) {
					if (len > 0) {
						for (int l = 0; l < len; l++) {
							// 生成参数类型进行检测代码，比如：
		                    // && $3[0].getName().equals("java.lang.String") 
							c3.append(" && ").append(" $3[").append(l).append("].getName().equals(\"")
									.append(m.getParameterTypes()[l].getName()).append("\")");
						}
					}
				}
				 // 添加 ) {，完成方法判断语句
				c3.append(" ) { ");
				// 上面的方法校验完成后的代码类似这样：
		        // if ("setName".equals($2) && $3.length == 1 && $3[0].getName().equals("java.lang.String")) {
				
				// 根据返回值类型生成目标方法调用语句
				if (m.getReturnType() == Void.TYPE) {
					//返回值类型为空，则 w.setName((java.lang.String)$4[0]); return null;
					c3.append(" w.").append(mn).append('(').append(args(m.getParameterTypes(), "$4")).append(");")
							.append(" return null;");
				} else {
					//返回值类型不为空，则  // return ($w)w.getName();
					c3.append(" return ($w)w.").append(mn).append('(').append(args(m.getParameterTypes(), "$4"))
							.append(");");
				}
				// 添加 },
				c3.append(" }");
				/* 上面的方法校验完成后的代码类似这样：
		        *if ("setName".equals($2) && $3.length == 1 && $3[0].getName().equals("java.lang.String")) {
		        *   w.setName((java.lang.String)$4[0]); return null;
		        *  }
		        */
				
				///添加方法名到集合中
				mns.add(mn);
				//如果是当前类声明的方法，则添加到声明方法集合中
				if (m.getDeclaringClass() == c) {
					dmns.add(mn);
				}
				//添加<方法描述,方法>到集合中
				ms.put(ReflectUtils.getDesc(m), m);
			}
			//添加异常捕获及抛出代码
			c3.append(" } catch(Throwable e) { ");
			c3.append("     throw new java.lang.reflect.InvocationTargetException(e); ");
			c3.append(" }");
		}
		//添加NoSuchMethodException异常代码
		c3.append(" throw new " + NoSuchMethodException.class.getName()
				+ "(\"Not found method \\\"\"+$2+\"\\\" in class " + c.getName() + ".\"); }");

		// 处理getter和setter方法
		Matcher matcher;
		for (Map.Entry<String, Method> entry : ms.entrySet()) {
			String md = entry.getKey();
			Method method = entry.getValue();
			//是否是get方法
			if ((matcher = ReflectUtils.GETTER_METHOD_DESC_PATTERN.matcher(md)).matches()) {
				//获取属性名
				String pn = propertyName(matcher.group(1));
				// 生成属性判断以及返回语句，示例如下：
	            // if( $2.equals("name") ) { return ($w).w.getName(); }
				c2.append(" if( $2.equals(\"").append(pn).append("\") ){ return ($w)w.").append(method.getName())
						.append("(); }");
				//存放<属性名，属性类型>到集合中
				pts.put(pn, method.getReturnType());
			//判断是否是is|has|can开头方法，生成代码逻辑同get方法
			} else if ((matcher = ReflectUtils.IS_HAS_CAN_METHOD_DESC_PATTERN.matcher(md)).matches()) {
				String pn = propertyName(matcher.group(1));
				c2.append(" if( $2.equals(\"").append(pn).append("\") ){ return ($w)w.").append(method.getName())
						.append("(); }");
				pts.put(pn, method.getReturnType());
			//判断是set开头方法
			} else if ((matcher = ReflectUtils.SETTER_METHOD_DESC_PATTERN.matcher(md)).matches()) {
				//获取参数(属性)类型
				Class<?> pt = method.getParameterTypes()[0];
				//获取属性名
				String pn = propertyName(matcher.group(1));
				//生成属性判断及set方法语句，示例如下：
				// if( $2.equals("name") ) { w.setName((java.lang.String)$3); return; }
				c1.append(" if( $2.equals(\"").append(pn).append("\") ){ w.").append(method.getName()).append("(")
						.append(arg(pt, "$3")).append("); return; }");
				//存放<属性名，属性类型>到集合中
				pts.put(pn, pt);
			}
		}
		// 添加 抛出 NoSuchPropertyException 异常代码
		c1.append(" throw new " + NoSuchPropertyException.class.getName()
				+ "(\"Not found property \\\"\"+$2+\"\\\" field or setter method in class " + c.getName() + ".\"); }");
		c2.append(" throw new " + NoSuchPropertyException.class.getName()
				+ "(\"Not found property \\\"\"+$2+\"\\\" field or setter method in class " + c.getName() + ".\"); }");

		// 生成class
		//包装类数量加1并获取，原子操作
		long id = WRAPPER_CLASS_COUNTER.getAndIncrement();
		//根据类加载器创建类生成器实例
		ClassGenerator cc = ClassGenerator.newInstance(cl);
		// 生成类名：有公共修饰符则org.apache.dubbo.common.bytecode.Wrapper，否则当前包装类名+$sw+当前包装类数量
		cc.setClassName((Modifier.isPublic(c.getModifiers()) ? Wrapper.class.getName() : c.getName() + "$sw") + id);
		// 设置父类
		cc.setSuperClass(Wrapper.class);
		// 设置默认构造函数
		cc.addDefaultConstructor();
		// 添加属性名称数组字段
		cc.addField("public static String[] pns;"); 
		// 属性即类类型字段
		cc.addField("public static " + Map.class.getName() + " pts;"); 
		// 添加方法名称集合属性
		cc.addField("public static String[] mns;");
		// 添加本类声明的方法名称集合字段
		cc.addField("public static String[] dmns;");
		// 添加类属性
		for (int i = 0, len = ms.size(); i < len; i++) {
			cc.addField("public static Class[] mts" + i + ";");
		}
		//添加属性名获取方法
		cc.addMethod("public String[] getPropertyNames(){ return pns; }");
		// 添加是否存在某个属性方法
		cc.addMethod("public boolean hasProperty(String n){ return pts.containsKey($1); }");
		// 获取某个属性类型
		cc.addMethod("public Class getPropertyType(String n){ return (Class)pts.get($1); }");
		// 获取方法名集合
		cc.addMethod("public String[] getMethodNames(){ return mns; }");
		//获取声明方法属性
		cc.addMethod("public String[] getDeclaredMethodNames(){ return dmns; }");
		// 添加setPropertyValue方法、getPropertyValue方法、invokeMethod方法代码
		cc.addMethod(c1.toString());
		cc.addMethod(c2.toString());
		cc.addMethod(c3.toString());

		try {
			Class<?> wc = cc.toClass();
			// 设置字段值
			wc.getField("pts").set(null, pts);
			wc.getField("pns").set(null, pts.keySet().toArray(new String[0]));
			wc.getField("mns").set(null, mns.toArray(new String[0]));
			wc.getField("dmns").set(null, dmns.toArray(new String[0]));
			int ix = 0;
			for (Method m : ms.values()) {
				wc.getField("mts" + ix++).set(null, m.getParameterTypes());
			}
			//创建Wrapper实例
			return (Wrapper) wc.newInstance();
		} catch (RuntimeException e) {
			throw e;
		} catch (Throwable e) {
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			//清理缓存
			cc.release();
			ms.clear();
			mns.clear();
			dmns.clear();
		}
	}

	private static String arg(Class<?> cl, String name) {
		if (cl.isPrimitive()) {
			if (cl == Boolean.TYPE) {
				return "((Boolean)" + name + ").booleanValue()";
			}
			if (cl == Byte.TYPE) {
				return "((Byte)" + name + ").byteValue()";
			}
			if (cl == Character.TYPE) {
				return "((Character)" + name + ").charValue()";
			}
			if (cl == Double.TYPE) {
				return "((Number)" + name + ").doubleValue()";
			}
			if (cl == Float.TYPE) {
				return "((Number)" + name + ").floatValue()";
			}
			if (cl == Integer.TYPE) {
				return "((Number)" + name + ").intValue()";
			}
			if (cl == Long.TYPE) {
				return "((Number)" + name + ").longValue()";
			}
			if (cl == Short.TYPE) {
				return "((Number)" + name + ").shortValue()";
			}
			throw new RuntimeException("Unknown primitive type: " + cl.getName());
		}
		return "(" + ReflectUtils.getName(cl) + ")" + name;
	}

	private static String args(Class<?>[] cs, String name) {
		int len = cs.length;
		if (len == 0) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			if (i > 0) {
				sb.append(',');
			}
			sb.append(arg(cs[i], name + "[" + i + "]"));
		}
		return sb.toString();
	}

	private static String propertyName(String pn) {
		return pn.length() == 1 || Character.isLowerCase(pn.charAt(1))
				? Character.toLowerCase(pn.charAt(0)) + pn.substring(1)
				: pn;
	}

	private static boolean hasMethods(Method[] methods) {
		if (methods == null || methods.length == 0) {
			return false;
		}
		for (Method m : methods) {
			if (m.getDeclaringClass() != Object.class) {
				return true;
			}
		}
		return false;
	}

	/**
	 * get property name array.
	 *
	 * @return property name array.
	 */
	abstract public String[] getPropertyNames();

	/**
	 * get property type.
	 *
	 * @param pn property name.
	 * @return Property type or nul.
	 */
	abstract public Class<?> getPropertyType(String pn);

	/**
	 * has property.
	 *
	 * @param name property name.
	 * @return has or has not.
	 */
	abstract public boolean hasProperty(String name);

	/**
	 * get property value.
	 *
	 * @param instance instance.
	 * @param pn       property name.
	 * @return value.
	 */
	abstract public Object getPropertyValue(Object instance, String pn)
			throws NoSuchPropertyException, IllegalArgumentException;

	/**
	 * set property value.
	 *
	 * @param instance instance.
	 * @param pn       property name.
	 * @param pv       property value.
	 */
	abstract public void setPropertyValue(Object instance, String pn, Object pv)
			throws NoSuchPropertyException, IllegalArgumentException;

	/**
	 * get property value.
	 *
	 * @param instance instance.
	 * @param pns      property name array.
	 * @return value array.
	 */
	public Object[] getPropertyValues(Object instance, String[] pns)
			throws NoSuchPropertyException, IllegalArgumentException {
		Object[] ret = new Object[pns.length];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = getPropertyValue(instance, pns[i]);
		}
		return ret;
	}

	/**
	 * set property value.
	 *
	 * @param instance instance.
	 * @param pns      property name array.
	 * @param pvs      property value array.
	 */
	public void setPropertyValues(Object instance, String[] pns, Object[] pvs)
			throws NoSuchPropertyException, IllegalArgumentException {
		if (pns.length != pvs.length) {
			throw new IllegalArgumentException("pns.length != pvs.length");
		}

		for (int i = 0; i < pns.length; i++) {
			setPropertyValue(instance, pns[i], pvs[i]);
		}
	}

	/**
	 * get method name array.
	 *
	 * @return method name array.
	 */
	abstract public String[] getMethodNames();

	/**
	 * get method name array.
	 *
	 * @return method name array.
	 */
	abstract public String[] getDeclaredMethodNames();

	/**
	 * has method.
	 *
	 * @param name method name.
	 * @return has or has not.
	 */
	public boolean hasMethod(String name) {
		for (String mn : getMethodNames()) {
			if (mn.equals(name)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * invoke method.
	 *
	 * @param instance instance.
	 * @param mn       method name.
	 * @param types
	 * @param args     argument array.
	 * @return return value.
	 */
	abstract public Object invokeMethod(Object instance, String mn, Class<?>[] types, Object[] args)
			throws NoSuchMethodException, InvocationTargetException;
}
