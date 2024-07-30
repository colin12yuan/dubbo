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
package org.apache.dubbo.registry.client;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.MetadataReportInstance;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.metrics.event.MetricsEventBus;
import org.apache.dubbo.metrics.metadata.event.MetadataEvent;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.registry.client.metadata.MetadataUtils;
import org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils;
import org.apache.dubbo.registry.client.metadata.store.MetaCacheManager;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_METADATA_INFO_CACHE_EXPIRE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_METADATA_INFO_CACHE_SIZE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.CommonConstants.METADATA_INFO_CACHE_EXPIRE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.METADATA_INFO_CACHE_SIZE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REGISTRY_LOCAL_FILE_CACHE_ENABLED;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.REGISTRY_FAILED_LOAD_METADATA;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_CLUSTER_KEY;
import static org.apache.dubbo.metadata.RevisionResolver.EMPTY_REVISION;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.EXPORTED_SERVICES_REVISION_PROPERTY_NAME;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getExportedServicesRevision;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.isValidInstance;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.setMetadataStorageType;

/**
 * Each service discovery is bond to one application.
 */
public abstract class AbstractServiceDiscovery implements ServiceDiscovery {
    private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(AbstractServiceDiscovery.class);
    private volatile boolean isDestroy;

    protected final String serviceName;
    protected volatile ServiceInstance serviceInstance;
    protected volatile MetadataInfo metadataInfo;
    protected final ConcurrentHashMap<String, MetadataInfoStat> metadataInfos = new ConcurrentHashMap<>();
    protected final ScheduledFuture<?> refreshCacheFuture;
    protected MetadataReport metadataReport;
    protected String metadataType;
    protected final MetaCacheManager metaCacheManager;
    protected URL registryURL;

    protected Set<ServiceInstancesChangedListener> instanceListeners = new ConcurrentHashSet<>();

    protected ApplicationModel applicationModel;

    public AbstractServiceDiscovery(ApplicationModel applicationModel, URL registryURL) {
        // 调用重载的构造器
        this(applicationModel, applicationModel.getApplicationName(), registryURL);

        // 元数据上报器
        MetadataReportInstance metadataReportInstance =
                applicationModel.getBeanFactory().getBean(MetadataReportInstance.class);
        metadataType = metadataReportInstance.getMetadataType();
        this.metadataReport = metadataReportInstance.getMetadataReport(registryURL.getParameter(REGISTRY_CLUSTER_KEY));
    }

    public AbstractServiceDiscovery(String serviceName, URL registryURL) {
        this(ApplicationModel.defaultModel(), serviceName, registryURL);
    }

    private AbstractServiceDiscovery(ApplicationModel applicationModel, String serviceName, URL registryURL) {
        this.applicationModel = applicationModel;
        this.serviceName = serviceName;
        this.registryURL = registryURL;
        // 服务元数据类：用来封装元数据信息
        this.metadataInfo = new MetadataInfo(serviceName);
        // 是否本地缓存配置
        boolean localCacheEnabled = registryURL.getParameter(REGISTRY_LOCAL_FILE_CACHE_ENABLED, true);
        // 这个是元数据缓存信息管理的类型，缓存文件使用 LRU 策略  感兴趣的可以详细看看
        // 缓存文件路径：
        this.metaCacheManager = new MetaCacheManager(
                localCacheEnabled,
                getCacheNameSuffix(),
                applicationModel
                        .getFrameworkModel()
                        .getBeanFactory()
                        .getBean(FrameworkExecutorRepository.class)
                        .getCacheRefreshingScheduledExecutor());
        int metadataInfoCacheExpireTime =
                registryURL.getParameter(METADATA_INFO_CACHE_EXPIRE_KEY, DEFAULT_METADATA_INFO_CACHE_EXPIRE);
        int metadataInfoCacheSize =
                registryURL.getParameter(METADATA_INFO_CACHE_SIZE_KEY, DEFAULT_METADATA_INFO_CACHE_SIZE);
        this.refreshCacheFuture = applicationModel
                .getFrameworkModel()
                .getBeanFactory()
                .getBean(FrameworkExecutorRepository.class)
                .getSharedScheduledExecutor()
                .scheduleAtFixedRate(
                        () -> {
                            try {
                                while (metadataInfos.size() > metadataInfoCacheSize) {
                                    AtomicReference<String> oldestRevision = new AtomicReference<>();
                                    AtomicReference<MetadataInfoStat> oldestStat = new AtomicReference<>();
                                    metadataInfos.forEach((k, v) -> {
                                        if (System.currentTimeMillis() - v.getUpdateTime() > metadataInfoCacheExpireTime
                                                && (oldestStat.get() == null
                                                        || oldestStat.get().getUpdateTime() > v.getUpdateTime())) {
                                            oldestRevision.set(k);
                                            oldestStat.set(v);
                                        }
                                    });
                                    if (oldestStat.get() != null) {
                                        metadataInfos.remove(oldestRevision.get(), oldestStat.get());
                                    }
                                }
                            } catch (Throwable t) {
                                logger.error(
                                        INTERNAL_ERROR, "", "", "Error occurred when clean up metadata info cache.", t);
                            }
                        },
                        metadataInfoCacheExpireTime / 2,
                        metadataInfoCacheExpireTime / 2,
                        TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void register() throws RuntimeException {
        if (isDestroy) {
            return;
        }
        if (this.serviceInstance == null) {
            // 创建应用的实例信息等待下面注册到注册中心
            ServiceInstance serviceInstance = createServiceInstance(this.metadataInfo);
            if (!isValidInstance(serviceInstance)) {
                return;
            }
            this.serviceInstance = serviceInstance;
        }
        // 开始创建版本号来判断是否需要更新。对应AbstractServiceDiscovery类型的calOrUpdateInstanceRevision
        boolean revisionUpdated = calOrUpdateInstanceRevision(this.serviceInstance);
        if (revisionUpdated) {
            // 服务发现：接口配置元数据
            reportMetadata(this.metadataInfo);
            // 应用的实例信息注册到注册中心之上
            doRegister(this.serviceInstance);
        }
    }

    /**
     * Update assumes that DefaultServiceInstance and its attributes will never get updated once created.
     * Checking hasExportedServices() before registration guarantees that at least one service is ready for creating the
     * instance.
     */
    @Override
    public synchronized void update() throws RuntimeException {
        if (isDestroy) {
            return;
        }

        if (this.serviceInstance == null) {
            register();
        }

        if (!isValidInstance(this.serviceInstance)) {
            return;
        }
        ServiceInstance oldServiceInstance = this.serviceInstance;
        DefaultServiceInstance newServiceInstance =
                new DefaultServiceInstance((DefaultServiceInstance) oldServiceInstance);
        boolean revisionUpdated = calOrUpdateInstanceRevision(newServiceInstance);
        if (revisionUpdated) {
            logger.info(String.format(
                    "Metadata of instance changed, updating instance with revision %s.",
                    newServiceInstance.getServiceMetadata().getRevision()));
            doUpdate(oldServiceInstance, newServiceInstance);
            this.serviceInstance = newServiceInstance;
        }
    }

    @Override
    public synchronized void unregister() throws RuntimeException {
        if (isDestroy) {
            return;
        }
        // fixme, this metadata info might still being shared by other instances
        //        unReportMetadata(this.metadataInfo);
        if (!isValidInstance(this.serviceInstance)) {
            return;
        }
        doUnregister(this.serviceInstance);
    }

    @Override
    public final ServiceInstance getLocalInstance() {
        return this.serviceInstance;
    }

    @Override
    public MetadataInfo getLocalMetadata() {
        return this.metadataInfo;
    }

    @Override
    public MetadataInfo getLocalMetadata(String revision) {
        MetadataInfoStat metadataInfoStat = metadataInfos.get(revision);
        if (metadataInfoStat != null) {
            return metadataInfoStat.getMetadataInfo();
        } else {
            return null;
        }
    }

    @Override
    public MetadataInfo getRemoteMetadata(String revision, List<ServiceInstance> instances) {
        MetadataInfo metadata = metaCacheManager.get(revision);

        if (metadata != null && metadata != MetadataInfo.EMPTY) {
            metadata.init();
            // metadata loaded from cache
            if (logger.isDebugEnabled()) {
                logger.debug("MetadataInfo for revision=" + revision + ", " + metadata);
            }
            return metadata;
        }

        synchronized (metaCacheManager) {
            // try to load metadata from remote.
            int triedTimes = 0;
            while (triedTimes < 3) {

                metadata = MetricsEventBus.post(
                        MetadataEvent.toSubscribeEvent(applicationModel),
                        () -> MetadataUtils.getRemoteMetadata(revision, instances, metadataReport),
                        result -> result != MetadataInfo.EMPTY);

                if (metadata != MetadataInfo.EMPTY) { // succeeded
                    metadata.init();
                    break;
                } else { // failed
                    if (triedTimes > 0) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Retry the " + triedTimes + " times to get metadata for revision=" + revision);
                        }
                    }
                    triedTimes++;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
            }

            if (metadata == MetadataInfo.EMPTY) {
                logger.error(
                        REGISTRY_FAILED_LOAD_METADATA,
                        "",
                        "",
                        "Failed to get metadata for revision after 3 retries, revision=" + revision);
            } else {
                metaCacheManager.put(revision, metadata);
            }
        }
        return metadata;
    }

    @Override
    public MetadataInfo getRemoteMetadata(String revision) {
        return metaCacheManager.get(revision);
    }

    @Override
    public final void destroy() throws Exception {
        isDestroy = true;
        metaCacheManager.destroy();
        refreshCacheFuture.cancel(true);
        doDestroy();
    }

    @Override
    public final boolean isDestroy() {
        return isDestroy;
    }

    @Override
    public void register(URL url) {
        // 用来操作元数据的
        metadataInfo.addService(url);
    }

    @Override
    public void unregister(URL url) {
        metadataInfo.removeService(url);
    }

    @Override
    public void subscribe(URL url, NotifyListener listener) {
        metadataInfo.addSubscribedURL(url);
    }

    @Override
    public void unsubscribe(URL url, NotifyListener listener) {
        metadataInfo.removeSubscribedURL(url);
    }

    @Override
    public List<URL> lookup(URL url) {
        throw new UnsupportedOperationException(
                "Service discovery implementation does not support lookup of url list.");
    }

    /**
     * Update Service Instance. Unregister and then register by default.
     * Can be override if registry support update instance directly.
     * <br/>
     * NOTICE: Remind to update {@link AbstractServiceDiscovery#serviceInstance}'s reference if updated
     * and report metadata by {@link AbstractServiceDiscovery#reportMetadata(MetadataInfo)}
     *
     * @param oldServiceInstance origin service instance
     * @param newServiceInstance new service instance
     */
    protected void doUpdate(ServiceInstance oldServiceInstance, ServiceInstance newServiceInstance) {
        this.doUnregister(oldServiceInstance);

        this.serviceInstance = newServiceInstance;

        if (!EMPTY_REVISION.equals(getExportedServicesRevision(newServiceInstance))) {
            reportMetadata(newServiceInstance.getServiceMetadata());
            this.doRegister(newServiceInstance);
        }
    }

    @Override
    public URL getUrl() {
        return registryURL;
    }

    protected abstract void doRegister(ServiceInstance serviceInstance) throws RuntimeException;

    protected abstract void doUnregister(ServiceInstance serviceInstance);

    protected abstract void doDestroy() throws Exception;

    /**
     * 这个方法的主要目的就是将应用的元数据信息都封装到 ServiceInstance 类型中，
     * 不过额外提供了一个扩展性比较好的方法可以自定义元数据信息
     * @param metadataInfo
     * @return
     */
    protected ServiceInstance createServiceInstance(MetadataInfo metadataInfo) {
        DefaultServiceInstance instance = new DefaultServiceInstance(serviceName, applicationModel);
        // 应用服务的元数据，可以看下面 debug 的数据信息
        instance.setServiceMetadata(metadataInfo);
        // metadataType的值为 local，这个方法是将元数据类型存储到应用的元数据对象中，对应内容为dubbo.metadata.storage-type:local
        setMetadataStorageType(instance, metadataType);
        // 这个是自定义元数据数据，我们也可以通过实现扩展 ServiceInstanceCustomizer 来自定义一些元数据
        ServiceInstanceMetadataUtils.customizeInstance(instance, applicationModel);
        return instance;
    }

    protected boolean calOrUpdateInstanceRevision(ServiceInstance instance) {
        // 获取元数据版本号对应字段 dubbo.metadata.revision
        String existingInstanceRevision = getExportedServicesRevision(instance);
        // 获取实例的服务元数据信
        MetadataInfo metadataInfo = instance.getServiceMetadata();
        // 这个方法其实比较重要，决定了什么时候会更新元数据，
        // 必须在不同线程之间同步计算此实例的状态，如同一实例的修订和修改。
        // 此方法的使用仅限于某些点，例如在注册期间。始终尝试使用此选项。
        // Dubbo 使用了一种 Hash 验证的方式将元数据转 MD5 值与之前的存在的版本号（也是元数据转MD5得到的）
        // 如果数据发生了变更则MD5值会发生变化，以此来更新元数据，不过发生了MD5冲突的话就会导致配置不更新这个冲突的概率非常小。
        String newRevision = metadataInfo.calAndGetRevision();
        if (!newRevision.equals(existingInstanceRevision)) {
            // 版本号添加到 dubbo.metadata.revision 字段中
            instance.getMetadata().put(EXPORTED_SERVICES_REVISION_PROPERTY_NAME, metadataInfo.getRevision());
            return true;
        }
        return false;
    }

    protected void reportMetadata(MetadataInfo metadataInfo) {
        if (metadataInfo == null) {
            return;
        }
        /**
         * `remote` - `Provider` 把 metadata 放到远端元数据中心，Consumer 从注册中心获取。
         * `local` - `Provider` 把 metadata 放在本地，Consumer 从 Provider 处直接获取 。
         */
        if (metadataReport != null) {
            SubscriberMetadataIdentifier identifier =
                    new SubscriberMetadataIdentifier(serviceName, metadataInfo.getRevision());
            // 是否远程发布元数据数据中心。服务发现：接口配置元数据
            if ((DEFAULT_METADATA_STORAGE_TYPE.equals(metadataType) && metadataReport.shouldReportMetadata())
                    || REMOTE_METADATA_STORAGE_TYPE.equals(metadataType)) {
                MetricsEventBus.post(MetadataEvent.toPushEvent(applicationModel), () -> {
                    metadataReport.publishAppMetadata(identifier, metadataInfo);
                    return null;
                });
            }
        }
        MetadataInfo clonedMetadataInfo = metadataInfo.clone();
        metadataInfos.put(metadataInfo.getRevision(), new MetadataInfoStat(clonedMetadataInfo));
    }

    protected void unReportMetadata(MetadataInfo metadataInfo) {
        if (metadataReport != null) {
            SubscriberMetadataIdentifier identifier =
                    new SubscriberMetadataIdentifier(serviceName, metadataInfo.getRevision());
            if ((DEFAULT_METADATA_STORAGE_TYPE.equals(metadataType) && metadataReport.shouldReportMetadata())
                    || REMOTE_METADATA_STORAGE_TYPE.equals(metadataType)) {
                metadataReport.unPublishAppMetadata(identifier, metadataInfo);
            }
        }
    }

    private String getCacheNameSuffix() {
        String name = this.getClass().getSimpleName();
        int i = name.indexOf(ServiceDiscovery.class.getSimpleName());
        if (i != -1) {
            name = name.substring(0, i);
        }
        StringBuilder stringBuilder = new StringBuilder(128);
        Optional<ApplicationConfig> application =
                applicationModel.getApplicationConfigManager().getApplication();
        if (application.isPresent()) {
            stringBuilder.append(application.get().getName());
            stringBuilder.append(".");
        }
        stringBuilder.append(name.toLowerCase());
        URL url = this.getUrl();
        if (url != null) {
            stringBuilder.append(".");
            stringBuilder.append(url.getBackupAddress());
        }
        return stringBuilder.toString();
    }

    private static class MetadataInfoStat {
        private final MetadataInfo metadataInfo;
        private final long updateTime = System.currentTimeMillis();

        public MetadataInfoStat(MetadataInfo metadataInfo) {
            this.metadataInfo = metadataInfo;
        }

        public MetadataInfo getMetadataInfo() {
            return metadataInfo;
        }

        public long getUpdateTime() {
            return updateTime;
        }
    }
}
