
package org.psr.spring.sql;

import static org.springframework.transaction.support.AbstractPlatformTransactionManager.SYNCHRONIZATION_ON_ACTUAL_TRANSACTION;

import java.lang.reflect.*;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.resource.cci.ConnectionFactory;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jca.cci.connection.CciLocalTransactionManager;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;

/**
 * Proxy class to handle multi-datasource Transaction Manager
 * @see PlatformTransactionManager
 * 
 */
public final class TransactionManagerWrapper implements InvocationHandler {
  /**
   * @param delegate 
   */
  private TransactionManagerWrapper(PlatformTransactionManager delegate) {
    this.delegate = delegate;
  }

  /**
   * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
   */
  @Override
  public Object invoke(Object proxy, Method method, Object[] methodArgs) throws Throwable {
    Object resource = getResource();
    if (resource instanceof DelegatingDataSource) {
      resource = ((DelegatingDataSource) resource).getTargetDataSource();
    }
    PlatformTransactionManager actualTransactionManager = resource == null
      ? delegate
      : resourceToTransactionManagerMap.computeIfAbsent(
        resource,
        TransactionManagerWrapper::getDefaultTransactionManager);
    if ("commit".equals(method.getName())) {
      boolean isGlobalRollbackOnly = methodArgs[0] instanceof DefaultTransactionStatus
        && ((DefaultTransactionStatus) methodArgs[0]).isGlobalRollbackOnly();
      try {
        return method.invoke(actualTransactionManager, methodArgs);
      }
      catch (InvocationTargetException e) {
        // suppress exception if GlobalRoleBack is set to true
        if (e.getCause() instanceof UnexpectedRollbackException && isGlobalRollbackOnly) {
          return null;
        }
        throw e.getCause();
      }
    }
    return method.invoke(actualTransactionManager, methodArgs);
  }

  /**
   * Register multi-resource transaction manager {@link PlatformTransactionManager}
   * <pre>
   *  Register transaction manager i.e
   *  &lt;tx:annotation-driven proxy-target-class="true" transaction-manager="transactionManager" order="10000"/&gt;
   *   
   *  Creating TransactionManager Bean i.e :
   *  &lt;bean id="actualTransactionManager" class="org.springframework.jdbc.datasource.DataSourceTransactionManager""&gt;
   *    &lt;property name="dataSource" ref="dataSourceRef" /"&gt;
   *  &lt;/bean"&gt;
   *   
   *  Creating proxy transaction manager i.e
   *  &lt;bean id="transactionManager" class="org.psr.spring.sql.TransactionManagerWrapper" factory-method="wrap"&gt;
   *    &lt;constructor-arg ref="actualTransactionManager"/&gt;
   *  &lt;/bean&gt;
   * </pre>
   * @param delegate
   * @return
   * @see TransactionManagerWrapper#addTransactionManager(PlatformTransactionManager)
   */
  public static PlatformTransactionManager wrap(PlatformTransactionManager delegate) {
    try {
      addTransactionManager(delegate);
      return (PlatformTransactionManager) Proxy.newProxyInstance(
        TransactionManagerWrapper.class.getClassLoader(),
        new Class<?>[] { PlatformTransactionManager.class },
        new TransactionManagerWrapper(delegate));
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to create wrapped TransactionManager", e);
    }
  }

  /**
   * 
   * add DataSourceTransactionManager
   * 
   * <pre>
   * 
   *  Creating TransactionManager Bean i.e :<br />
   *  &lt;bean id="actualTransactionManager" class="org.springframework.jdbc.datasource.DataSourceTransactionManager""&gt;
   *    &lt;property name="dataSource" ref="dataSourceRef" /"&gt;
   *  &lt;/bean"&gt;
   *  <br /> 
   *  Registering transaction manager i.e:<br />
   *  &lt;bean class="org.springframework.beans.factory.config.MethodInvokingFactoryBean"&gt;
   *    &lt;property name="targetClass" value="org.psr.spring.sql.TransactionManagerWrapper" /&gt;
   *    &lt;property name="targetMethod" value="addTransactionManager" /&gt;
   *    &lt;property name="arguments"&gt;
   *      &lt;array&gt;
   *        &lt;ref bean="actualTransactionManager"/&gt;
   *      &lt;/array&gt;
   *    &lt;/property&gt;
   *  &lt;/bean&gt;
   * </pre>
   *
   * @param delegate
   */
  public static void addTransactionManager(PlatformTransactionManager transactionManager) {
    Object resource = null;
    if (transactionManager instanceof ResourceTransactionManager) {
      resource = ((ResourceTransactionManager) transactionManager).getResourceFactory();
    }
    else if (resource instanceof JtaTransactionManager) {
      JtaTransactionManager jtaTransactionManager = (JtaTransactionManager) transactionManager;
      resource = jtaTransactionManager.getUserTransaction();
      resourceToTransactionManagerMap.putIfAbsent(jtaTransactionManager.getTransactionManager(), transactionManager);
    }
    if (resource == null) {
      LOGGER.warn("Unknown Transaction Manager: " + transactionManager.getClass());
    }
    else {
      resourceToTransactionManagerMap.putIfAbsent(resource, transactionManager);
    }

  }

  private static PlatformTransactionManager getDefaultTransactionManager(final Object resource) {
    Objects.requireNonNull(resource, "Resource is required cannot be null");
    PlatformTransactionManager transactionManager = null;
    if (resource instanceof DataSource) {
      transactionManager = new DataSourceTransactionManager((DataSource) resource);
    }
    else if (resource instanceof ConnectionFactory) {
      transactionManager = new CciLocalTransactionManager((ConnectionFactory) resource);
    }
    else if (resource instanceof UserTransaction) {
      transactionManager = new JtaTransactionManager((UserTransaction) resource);
    }
    else if (resource instanceof TransactionManager) {
      transactionManager = new JtaTransactionManager((TransactionManager) resource);
    }
    else if (resource instanceof PlatformTransactionManager) {
      transactionManager = (PlatformTransactionManager) resource;
    }
    else {
      StringBuilder errorBuilder = new StringBuilder();
      errorBuilder.append("Illegal Argument: ").append(resource.getClass().getName());
      errorBuilder.append(System.lineSeparator()).append("Supported Type are:");
      errorBuilder.append(System.lineSeparator()).append(DataSource.class.getName());
      errorBuilder.append(System.lineSeparator()).append(ConnectionFactory.class.getName());
      errorBuilder.append(System.lineSeparator()).append(UserTransaction.class.getName());
      errorBuilder.append(System.lineSeparator()).append(TransactionManager.class.getName());
      errorBuilder.append(System.lineSeparator()).append(PlatformTransactionManager.class.getName());
      throw new IllegalArgumentException(errorBuilder.toString());
    }
    if (transactionManager instanceof AbstractPlatformTransactionManager) {
      ((AbstractPlatformTransactionManager) transactionManager).setTransactionSynchronization(SYNCHRONIZATION_ON_ACTUAL_TRANSACTION);
    }
    return transactionManager;
  }
 
  /**
   * 
   * Get current resource
   *
   * @return
   */
  private static Object getResource() {
    //TODO: logic to get the current Datasource or ConnectionFactory or UserTransaction or TransactionManager;
    return null;
  }

  public final PlatformTransactionManager delegate;
  private static final Map<Object, PlatformTransactionManager> resourceToTransactionManagerMap = new ConcurrentHashMap<>();
  private static final Log LOGGER = LogFactory.getLog(TransactionManagerWrapper.class);
}
