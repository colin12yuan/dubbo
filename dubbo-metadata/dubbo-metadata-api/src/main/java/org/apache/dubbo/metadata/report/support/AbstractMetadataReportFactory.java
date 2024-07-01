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
package org.apache.dubbo.metadata.report.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.MetadataReportFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.dubbo.common.constants.CommonConstants.CHECK_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.COMMON_UNEXPECTED_EXCEPTION;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROXY_FAILED_EXPORT_SERVICE;

public abstract class AbstractMetadataReportFactory implements MetadataReportFactory {

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(AbstractMetadataReportFactory.class);
    private static final String EXPORT_KEY = "export";
    private static final String REFER_KEY = "refer";

    /**
     * The lock for the acquisition process of the registry
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Registry Collection Map<metadataAddress, MetadataReport>
     */
    private final Map<String, MetadataReport> serviceStoreMap = new ConcurrentHashMap<>();

    @Override
    public MetadataReport getMetadataReport(URL url) {
        // url值参考例子 zookeeper://127.0.0.1:2181?application=dubbo-demo-api-provider&client=&port=2181&protocol=zookeeper
        // 如果存在 export 则移除
        url = url.setPath(MetadataReport.class.getName()).removeParameters(EXPORT_KEY, REFER_KEY);
        // 生成元数据缓存key元数据维度 地址+名字
        // 如: zookeeper://127.0.0.1:2181/org.apache.dubbo.metadata.report.MetadataReport
        String key = toMetadataReportKey(url);
        // 缓存中查询 查到则直接返回
        MetadataReport metadataReport = serviceStoreMap.get(key);
        if (metadataReport != null) {
            return metadataReport;
        }

        // Lock the metadata access process to ensure a single instance of the metadata instance
        lock.lock();
        try {
            // 双重校验锁在查一下
            metadataReport = serviceStoreMap.get(key);
            if (metadataReport != null) {
                return metadataReport;
            }
            // check 参数，查元数据报错是否抛出异常
            boolean check = url.getParameter(CHECK_KEY, true) && url.getPort() != 0;
            try {
                // 关键模版方法，调用扩展实现的具体业务(创建元数据操作对象)
                metadataReport = createMetadataReport(url);
            } catch (Exception e) {
                if (!check) {
                    logger.warn(PROXY_FAILED_EXPORT_SERVICE, "", "", "The metadata reporter failed to initialize", e);
                } else {
                    throw e;
                }
            }

            if (check && metadataReport == null) {
                throw new IllegalStateException("Can not create metadata Report " + url);
            }
            if (metadataReport != null) {
                serviceStoreMap.put(key, metadataReport);
            }
            return metadataReport;
        } finally {
            // Release the lock
            lock.unlock();
        }
    }

    protected String toMetadataReportKey(URL url) {
        return url.toServiceString();
    }

    @Override
    public void destroy() {
        lock.lock();
        try {
            for (MetadataReport metadataReport : serviceStoreMap.values()) {
                try {
                    metadataReport.destroy();
                } catch (Throwable ignored) {
                    // ignored
                    logger.warn(COMMON_UNEXPECTED_EXCEPTION, "", "", ignored.getMessage(), ignored);
                }
            }
            serviceStoreMap.clear();
        } finally {
            lock.unlock();
        }
    }

    protected abstract MetadataReport createMetadataReport(URL url);
}
