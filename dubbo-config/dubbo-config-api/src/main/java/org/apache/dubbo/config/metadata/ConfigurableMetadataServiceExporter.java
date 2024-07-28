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
package org.apache.dubbo.config.metadata;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ArgumentConfig;
import org.apache.dubbo.config.MethodConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.config.bootstrap.builders.InternalServiceConfigBuilder;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.registry.client.metadata.MetadataServiceDelegation;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static java.util.Collections.emptyList;
import static org.apache.dubbo.common.constants.CommonConstants.METADATA_SERVICE_PORT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.METADATA_SERVICE_PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.CONFIG_METADATA_SERVICE_EXPORTED;

/**
 * Export metadata service
 */
public class ConfigurableMetadataServiceExporter {

    private final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    private MetadataServiceDelegation metadataService;

    private volatile ServiceConfig<MetadataService> serviceConfig;
    private final ApplicationModel applicationModel;

    public ConfigurableMetadataServiceExporter(
            ApplicationModel applicationModel, MetadataServiceDelegation metadataService) {
        this.applicationModel = applicationModel;
        this.metadataService = metadataService;
    }

    public synchronized ConfigurableMetadataServiceExporter export() {
        // 元数据服务配置已经存在或者已经导出或者不可导出情况下是无需导出的
        if (serviceConfig == null || !isExported()) {
            ExecutorService internalServiceExecutor = applicationModel
                    .getFrameworkModel()
                    .getBeanFactory()
                    .getBean(FrameworkExecutorRepository.class)
                    .getInternalServiceExecutor();
            // 创建服务配置
            /* 这个元数据服务对象有几个比较特殊的配置
             * 1、注册中心的配置register设置为了false 则为不向注册中心注册具体的服务配置信息
             */
            this.serviceConfig = InternalServiceConfigBuilder.<MetadataService>newBuilder(applicationModel)
                    .interfaceClass(MetadataService.class)
                    .protocol(getApplicationConfig().getMetadataServiceProtocol(), METADATA_SERVICE_PROTOCOL_KEY)
                    .port(getApplicationConfig().getMetadataServicePort(), METADATA_SERVICE_PORT_KEY)
                    .registryId("internal-metadata-registry")
                    .executor(internalServiceExecutor)
                    // 这里见传入参数：这里为 MetadataServiceDelegation
                    .ref(metadataService)
                    // 见 build 实现，生成方法配置。这里目前提供的服务方法为 getAndListenInstanceMetadata 方法。
                    // 即通过 MetadataServiceDelegation#getAndListenInstanceMetadata
                    .build(configConsumer -> configConsumer.setMethods(generateMethodConfig()));

            // export
            // 导出服务
            serviceConfig.export();

            metadataService.setMetadataURL(serviceConfig.getExportedUrls().get(0));
            if (logger.isInfoEnabled()) {
                logger.info("The MetadataService exports urls : " + serviceConfig.getExportedUrls());
            }
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn(
                        CONFIG_METADATA_SERVICE_EXPORTED,
                        "",
                        "",
                        "The MetadataService has been exported : " + serviceConfig.getExportedUrls());
            }
        }

        return this;
    }

    public ConfigurableMetadataServiceExporter unexport() {
        if (isExported()) {
            serviceConfig.unexport();
            metadataService.setMetadataURL(null);
        }
        return this;
    }

    public boolean isExported() {
        return serviceConfig != null && serviceConfig.isExported() && !serviceConfig.isUnexported();
    }

    private ApplicationConfig getApplicationConfig() {
        return applicationModel.getApplicationConfigManager().getApplication().get();
    }

    /**
     * Generate Method Config for Service Discovery Metadata <p/>
     * <p>
     * Make {@link MetadataService} support argument callback,
     * used to notify {@link org.apache.dubbo.registry.client.ServiceInstance}'s
     * metadata change event
     *
     * @since 3.0
     */
    private List<MethodConfig> generateMethodConfig() {
        MethodConfig methodConfig = new MethodConfig();
        methodConfig.setName("getAndListenInstanceMetadata");

        ArgumentConfig argumentConfig = new ArgumentConfig();
        argumentConfig.setIndex(1);
        argumentConfig.setCallback(true);

        methodConfig.setArguments(Collections.singletonList(argumentConfig));

        return Collections.singletonList(methodConfig);
    }

    // for unit test
    public void setMetadataService(MetadataServiceDelegation metadataService) {
        this.metadataService = metadataService;
    }

    // for unit test
    public List<URL> getExportedURLs() {
        return serviceConfig != null ? serviceConfig.getExportedUrls() : emptyList();
    }
}
