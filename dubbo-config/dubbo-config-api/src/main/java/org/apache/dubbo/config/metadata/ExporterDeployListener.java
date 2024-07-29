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

import org.apache.dubbo.common.deploy.ApplicationDeployListener;
import org.apache.dubbo.common.lang.Prioritized;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.registry.client.metadata.MetadataServiceDelegation;
import org.apache.dubbo.rpc.model.ApplicationModel;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_REGISTER_MODE;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_REGISTER_MODE;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_METADATA_STORAGE_TYPE;

public class ExporterDeployListener implements ApplicationDeployListener, Prioritized {
    protected volatile ConfigurableMetadataServiceExporter metadataServiceExporter;

    @Override
    public void onInitialize(ApplicationModel scopeModel) {}

    @Override
    public void onStarting(ApplicationModel scopeModel) {}

    @Override
    public synchronized void onStarted(ApplicationModel applicationModel) {}

    @Override
    public synchronized void onStopping(ApplicationModel scopeModel) {}

    private String getMetadataType(ApplicationModel applicationModel) {
        String type = applicationModel
                .getApplicationConfigManager()
                .getApplicationOrElseThrow()
                .getMetadataType();
        if (StringUtils.isEmpty(type)) {
            type = DEFAULT_METADATA_STORAGE_TYPE;
        }
        return type;
    }

    private String getRegisterMode(ApplicationModel applicationModel) {
        String type = applicationModel
                .getApplicationConfigManager()
                .getApplicationOrElseThrow()
                .getRegisterMode();
        if (StringUtils.isEmpty(type)) {
            type = DEFAULT_REGISTER_MODE;
        }
        return type;
    }

    public ConfigurableMetadataServiceExporter getMetadataServiceExporter() {
        return metadataServiceExporter;
    }

    public void setMetadataServiceExporter(ConfigurableMetadataServiceExporter metadataServiceExporter) {
        this.metadataServiceExporter = metadataServiceExporter;
    }

    @Override
    public synchronized void onModuleStarted(ApplicationModel applicationModel) {
        // start metadata service exporter
        // MetadataServiceDelegation 类型为实现提供远程 RPC 服务以方便元数据信息的查询功能的类型。
        MetadataServiceDelegation metadataService =
                applicationModel.getBeanFactory().getOrRegisterBean(MetadataServiceDelegation.class);
        if (metadataServiceExporter == null) {
            metadataServiceExporter = new ConfigurableMetadataServiceExporter(applicationModel, metadataService);
            // fixme, let's disable local metadata service export at this moment
            /* metadata-type
             * metadata 传递方式，是以 Provider 视角而言的，Consumer 侧配置无效，可选值有：
             * remote - Provider 把 metadata 放到远端注册中心，Consumer 从注册中心获取。
             * local - Provider 把 metadata 放在本地，Consumer 从 Provider 处直接获取 。
             * 可以看到默认的local配置元数据信息的获取是由消费者从提供者拉的，
             * 那提供者怎么拉取对应服务的元数据信息那就要要用到这个博客说到的MetadataService服务，
             * 传递方式为 remote 的方式其实就要依赖注册中心了相对来说增加了注册中心的压力 */
            // 默认我们是没有配置这个元数据类型的，这里元数据类型默认为 local，条件不是 remote 则处理
            // 注册模式：若是接口级注册模式，则不需要元数据中心暴露服务元数据信息。接口注册模式为 dubbo 2.6.x 及以前注册方式。
            if (!REMOTE_METADATA_STORAGE_TYPE.equals(getMetadataType(applicationModel))
                    && !INTERFACE_REGISTER_MODE.equals(getRegisterMode(applicationModel))) {
                // 元数据服务导出
                metadataServiceExporter.export();
            }
        }
    }

    @Override
    public synchronized void onStopped(ApplicationModel scopeModel) {
        if (metadataServiceExporter != null && metadataServiceExporter.isExported()) {
            try {
                metadataServiceExporter.unexport();
            } catch (Exception ignored) {
                // ignored
            }
        }
    }

    @Override
    public void onFailure(ApplicationModel scopeModel, Throwable cause) {}

    @Override
    public int getPriority() {
        return MAX_PRIORITY;
    }
}
