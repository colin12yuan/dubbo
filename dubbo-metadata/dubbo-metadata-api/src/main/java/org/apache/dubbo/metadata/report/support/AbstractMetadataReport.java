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
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.JsonUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.metadata.definition.model.FullServiceDefinition;
import org.apache.dubbo.metadata.definition.model.ServiceDefinition;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.identifier.KeyTypeEnum;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.metrics.event.MetricsEventBus;
import org.apache.dubbo.metrics.metadata.event.MetadataEvent;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.CYCLE_REPORT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.FILE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.REGISTRY_LOCAL_FILE_CACHE_ENABLED;
import static org.apache.dubbo.common.constants.CommonConstants.REPORT_DEFINITION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REPORT_METADATA_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RETRY_PERIOD_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RETRY_TIMES_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SYNC_REPORT_KEY;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.COMMON_UNEXPECTED_EXCEPTION;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROXY_FAILED_EXPORT_SERVICE;
import static org.apache.dubbo.common.utils.StringUtils.replace;
import static org.apache.dubbo.metadata.report.support.Constants.CACHE;
import static org.apache.dubbo.metadata.report.support.Constants.DEFAULT_METADATA_REPORT_CYCLE_REPORT;
import static org.apache.dubbo.metadata.report.support.Constants.DEFAULT_METADATA_REPORT_RETRY_PERIOD;
import static org.apache.dubbo.metadata.report.support.Constants.DEFAULT_METADATA_REPORT_RETRY_TIMES;
import static org.apache.dubbo.metadata.report.support.Constants.DUBBO_METADATA;
import static org.apache.dubbo.metadata.report.support.Constants.USER_HOME;

public abstract class AbstractMetadataReport implements MetadataReport {

    protected static final String DEFAULT_ROOT = "dubbo";

    private static final int ONE_DAY_IN_MILLISECONDS = 60 * 24 * 60 * 1000;
    private static final int FOUR_HOURS_IN_MILLISECONDS = 60 * 4 * 60 * 1000;
    // Log output
    protected final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

    // Local disk cache, where the special key value.registries records the list of metadata centers, and the others are
    // the list of notified service providers
    final Properties properties = new Properties();
    private final ExecutorService reportCacheExecutor =
            Executors.newFixedThreadPool(1, new NamedThreadFactory("DubboSaveMetadataReport", true));
    final Map<MetadataIdentifier, Object> allMetadataReports = new ConcurrentHashMap<>(4);

    private final AtomicLong lastCacheChanged = new AtomicLong();
    final Map<MetadataIdentifier, Object> failedReports = new ConcurrentHashMap<>(4);
    private URL reportURL;
    boolean syncReport;
    // Local disk cache file
    File file;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    public MetadataReportRetry metadataReportRetry;
    private ScheduledExecutorService reportTimerScheduler;

    private final boolean reportMetadata;
    private final boolean reportDefinition;
    protected ApplicationModel applicationModel;

    public AbstractMetadataReport(URL reportServerURL) {
        // 设置url 如:zookeeper://127.0.0.1:2181/org.apache.dubbo.metadata.report.MetadataReport?application=dubbo-demo-api-provider&client=&port=2181&protocol=zookeeper
        setUrl(reportServerURL);
        applicationModel = reportServerURL.getOrDefaultApplicationModel();

        boolean localCacheEnabled = reportServerURL.getParameter(REGISTRY_LOCAL_FILE_CACHE_ENABLED, true);
        // Start file save timer
        // 缓存的文件名字
        // 格式为: 用户目录+/.dubbo/dubbo-metadata- + 应用程序名字application + url地址(IP+端口) + 后缀.cache 如下所示
        // /Users/song/.dubbo/dubbo-metadata-dubbo-demo-api-provider-127.0.0.1-2181.cache
        String defaultFilename = System.getProperty(USER_HOME) + DUBBO_METADATA + reportServerURL.getApplication()
                + "-" + replace(reportServerURL.getAddress(), ":", "-")
                + CACHE;
        // 如果用户配置了缓存文件名字则以用户配置为准file
        String filename = reportServerURL.getParameter(FILE_KEY, defaultFilename);
        File file = null;
        if (localCacheEnabled && ConfigUtils.isNotEmpty(filename)) {
            file = new File(filename);
            if (!file.exists()
                    && file.getParentFile() != null
                    && !file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    throw new IllegalArgumentException("Invalid service store file " + file
                            + ", cause: Failed to create directory " + file.getParentFile() + "!");
                }
            }
            // if this file exists, firstly delete it.
            if (!initialized.getAndSet(true) && file.exists()) {
                file.delete();
            }
        }
        // 赋值给成员变量后续继续可以用
        this.file = file;
        // 文件存在则直接加载文件中的内容
        loadProperties();
        // sync-report 配置的值为同步配置还异步配置，true是同步配置，默认为false为异步配置
        syncReport = reportServerURL.getParameter(SYNC_REPORT_KEY, false);
        // 重试属性与逻辑也封装了一个类型，创建对象
        // retry-times重试次数配置 默认为100次
        // retry-period 重试间隔配置 默认为3000
        metadataReportRetry = new MetadataReportRetry(
                reportServerURL.getParameter(RETRY_TIMES_KEY, DEFAULT_METADATA_REPORT_RETRY_TIMES),
                reportServerURL.getParameter(RETRY_PERIOD_KEY, DEFAULT_METADATA_REPORT_RETRY_PERIOD));
        // cycle report the data switch
        // 是否定期从元数据中心同步配置
        // -report配置默认为true
        if (reportServerURL.getParameter(CYCLE_REPORT_KEY, DEFAULT_METADATA_REPORT_CYCLE_REPORT)) {
            // 开启重试定时器
            reportTimerScheduler = Executors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory("DubboMetadataReportTimer", true));
            reportTimerScheduler.scheduleAtFixedRate(
                    this::publishAll, calculateStartTime(), ONE_DAY_IN_MILLISECONDS, TimeUnit.MILLISECONDS);
        }

        this.reportMetadata = reportServerURL.getParameter(REPORT_METADATA_KEY, false);
        this.reportDefinition = reportServerURL.getParameter(REPORT_DEFINITION_KEY, true);
    }

    public URL getUrl() {
        return reportURL;
    }

    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("metadataReport url == null");
        }
        this.reportURL = url;
    }

    private void doSaveProperties(long version) {
        // 不是最新的就不要持久化了
        if (version < lastCacheChanged.get()) {
            return;
        }
        if (file == null) {
            return;
        }
        // Save
        try {
            // 创建本地文件锁:
            // 路径为:
            // /Users/song/.dubbo/dubbo-metadata-dubbo-demo-api-provider-127.0.0.1-2181.cache.lock
            File lockfile = new File(file.getAbsolutePath() + ".lock");
            // 锁文件不存在则创建锁文件
            if (!lockfile.exists()) {
                lockfile.createNewFile();
            }
            // 随机访问文件工具类对象创建 读写权限
            try (RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");
                    // 文件Channel，返回与此文件关联的唯一FileChannel对象。
                    FileChannel channel = raf.getChannel()) {
                // FileChannel中的lock()与tryLock()方法都是尝试去获取在某一文件上的独有锁（以下简称独有锁），可以实现进程间操作的互斥。
                // 区别在于lock()会阻塞（blocking）方法的执行，tryLock()则不会。
                FileLock lock = channel.tryLock();
                // 如果多个线程同时进来未获取锁的则抛出异常
                if (lock == null) {
                    throw new IOException(
                            "Can not lock the metadataReport cache file " + file.getAbsolutePath()
                                    + ", ignore and retry later, maybe multi java process use the file, please config: dubbo.metadata.file=xxx.properties");
                }
                // Save
                try {
                    // 文件不存在则创建本地元数据缓存文件
                    // /Users/song/.dubbo/dubbo-metadata-dubbo-demo-api-provider-127.0.0.1-2181.cache
                    if (!file.exists()) {
                        file.createNewFile();
                    }

                    Properties tmpProperties;
                    if (!syncReport) {
                        // When syncReport = false, properties.setProperty and properties.store are called from the same
                        // thread(reportCacheExecutor), so deep copy is not required
                        tmpProperties = properties;
                    } else {
                        // Using store method and setProperty method of the this.properties will cause lock contention
                        // under multi-threading, so deep copy a new container
                        // 异步存储会导致锁争用，使用此的store方法和setProperty方法。属性将导致多线程下的锁争用，因此深度复制新容器
                        tmpProperties = new Properties();
                        Set<Map.Entry<Object, Object>> entries = properties.entrySet();
                        for (Map.Entry<Object, Object> entry : entries) {
                            tmpProperties.setProperty((String) entry.getKey(), (String) entry.getValue());
                        }
                    }

                    try (FileOutputStream outputFile = new FileOutputStream(file)) {
                        // Properties类型自带的方法:
                        // 将此属性表中的属性列表（键和元素对）以适合使用load（Reader）方法的格式写入输出字符流。
                        tmpProperties.store(outputFile, "Dubbo metadataReport Cache");
                    }
                } finally {
                    lock.release();
                }
            }
        } catch (Throwable e) {
            if (version < lastCacheChanged.get()) {
                return;
            } else {
                reportCacheExecutor.execute(new SaveProperties(lastCacheChanged.incrementAndGet()));
            }
            logger.warn(
                    COMMON_UNEXPECTED_EXCEPTION,
                    "",
                    "",
                    "Failed to save service store file, cause: " + e.getMessage(),
                    e);
        }
    }

    void loadProperties() {
        if (file != null && file.exists()) {
            try (InputStream in = new FileInputStream(file)) {
                properties.load(in);
                if (logger.isInfoEnabled()) {
                    logger.info("Load service store file " + file + ", data: " + properties);
                }
            } catch (Throwable e) {
                logger.warn(COMMON_UNEXPECTED_EXCEPTION, "", "", "Failed to load service store file" + file, e);
            }
        }
    }

    private void saveProperties(MetadataIdentifier metadataIdentifier, String value, boolean add, boolean sync) {
        if (file == null) {
            return;
        }

        try {
            if (add) {
                properties.setProperty(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY), value);
            } else {
                properties.remove(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
            }
            // 获取最新修改版本持久化到磁盘
            long version = lastCacheChanged.incrementAndGet();
            if (sync) {
                new SaveProperties(version).run();
            } else {
                reportCacheExecutor.execute(new SaveProperties(version));
            }

        } catch (Throwable t) {
            logger.warn(COMMON_UNEXPECTED_EXCEPTION, "", "", t.getMessage(), t);
        }
    }

    @Override
    public String toString() {
        return getUrl().toString();
    }

    private class SaveProperties implements Runnable {
        private long version;

        private SaveProperties(long version) {
            this.version = version;
        }

        @Override
        public void run() {
            doSaveProperties(version);
        }
    }

    @Override
    public void storeProviderMetadata(
            MetadataIdentifier providerMetadataIdentifier, ServiceDefinition serviceDefinition) {
        // 是否同步配置对应 sync-report，默认为异步
        if (syncReport) {
            storeProviderMetadataTask(providerMetadataIdentifier, serviceDefinition);
        } else {
            reportCacheExecutor.execute(() -> storeProviderMetadataTask(providerMetadataIdentifier, serviceDefinition));
        }
    }

    private void storeProviderMetadataTask(
            MetadataIdentifier providerMetadataIdentifier, ServiceDefinition serviceDefinition) {

        MetadataEvent metadataEvent = MetadataEvent.toServiceSubscribeEvent(
                applicationModel, providerMetadataIdentifier.getUniqueServiceName());
        MetricsEventBus.post(
                metadataEvent,
                () -> {
                    boolean result = true;
                    try {
                        if (logger.isInfoEnabled()) {
                            logger.info("store provider metadata. Identifier : " + providerMetadataIdentifier
                                    + "; definition: " + serviceDefinition);
                        }
                        allMetadataReports.put(providerMetadataIdentifier, serviceDefinition);
                        failedReports.remove(providerMetadataIdentifier);
                        String data = JsonUtils.toJson(serviceDefinition);
                        // 内存中的元数据同步到元数据中心
                        doStoreProviderMetadata(providerMetadataIdentifier, data);
                        // 内存中的元数据同步到本地文件
                        saveProperties(providerMetadataIdentifier, data, true, !syncReport);
                    } catch (Exception e) {
                        // retry again. If failed again, throw exception.
                        failedReports.put(providerMetadataIdentifier, serviceDefinition);
                        metadataReportRetry.startRetryTask();
                        logger.error(
                                PROXY_FAILED_EXPORT_SERVICE,
                                "",
                                "",
                                "Failed to put provider metadata " + providerMetadataIdentifier + " in  "
                                        + serviceDefinition + ", cause: " + e.getMessage(),
                                e);
                        result = false;
                    }
                    return result;
                },
                aBoolean -> aBoolean);
    }

    @Override
    public void storeConsumerMetadata(
            MetadataIdentifier consumerMetadataIdentifier, Map<String, String> serviceParameterMap) {
        if (syncReport) {
            storeConsumerMetadataTask(consumerMetadataIdentifier, serviceParameterMap);
        } else {
            reportCacheExecutor.execute(
                    () -> storeConsumerMetadataTask(consumerMetadataIdentifier, serviceParameterMap));
        }
    }

    protected void storeConsumerMetadataTask(
            MetadataIdentifier consumerMetadataIdentifier, Map<String, String> serviceParameterMap) {
        try {
            if (logger.isInfoEnabled()) {
                logger.info("store consumer metadata. Identifier : " + consumerMetadataIdentifier + "; definition: "
                        + serviceParameterMap);
            }
            allMetadataReports.put(consumerMetadataIdentifier, serviceParameterMap);
            failedReports.remove(consumerMetadataIdentifier);

            String data = JsonUtils.toJson(serviceParameterMap);
            doStoreConsumerMetadata(consumerMetadataIdentifier, data);
            saveProperties(consumerMetadataIdentifier, data, true, !syncReport);
        } catch (Exception e) {
            // retry again. If failed again, throw exception.
            failedReports.put(consumerMetadataIdentifier, serviceParameterMap);
            metadataReportRetry.startRetryTask();
            logger.error(
                    PROXY_FAILED_EXPORT_SERVICE,
                    "",
                    "",
                    "Failed to put consumer metadata " + consumerMetadataIdentifier + ";  " + serviceParameterMap
                            + ", cause: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public void destroy() {
        if (reportCacheExecutor != null) {
            reportCacheExecutor.shutdown();
        }
        if (reportTimerScheduler != null) {
            reportTimerScheduler.shutdown();
        }
        if (metadataReportRetry != null) {
            metadataReportRetry.destroy();
            metadataReportRetry = null;
        }
    }

    @Override
    public void saveServiceMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url) {
        if (syncReport) {
            doSaveMetadata(metadataIdentifier, url);
        } else {
            reportCacheExecutor.execute(() -> doSaveMetadata(metadataIdentifier, url));
        }
    }

    @Override
    public void removeServiceMetadata(ServiceMetadataIdentifier metadataIdentifier) {
        if (syncReport) {
            doRemoveMetadata(metadataIdentifier);
        } else {
            reportCacheExecutor.execute(() -> doRemoveMetadata(metadataIdentifier));
        }
    }

    @Override
    public List<String> getExportedURLs(ServiceMetadataIdentifier metadataIdentifier) {
        // TODO, fallback to local cache
        return doGetExportedURLs(metadataIdentifier);
    }

    @Override
    public void saveSubscribedData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, Set<String> urls) {
        if (syncReport) {
            doSaveSubscriberData(subscriberMetadataIdentifier, JsonUtils.toJson(urls));
        } else {
            reportCacheExecutor.execute(
                    () -> doSaveSubscriberData(subscriberMetadataIdentifier, JsonUtils.toJson(urls)));
        }
    }

    @Override
    public List<String> getSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier) {
        String content = doGetSubscribedURLs(subscriberMetadataIdentifier);
        return JsonUtils.toJavaList(content, String.class);
    }

    String getProtocol(URL url) {
        String protocol = url.getSide();
        protocol = protocol == null ? url.getProtocol() : protocol;
        return protocol;
    }

    /**
     * @return if need to continue
     */
    public boolean retry() {
        return doHandleMetadataCollection(failedReports);
    }

    @Override
    public boolean shouldReportDefinition() {
        return reportDefinition;
    }

    @Override
    public boolean shouldReportMetadata() {
        return reportMetadata;
    }

    private boolean doHandleMetadataCollection(Map<MetadataIdentifier, Object> metadataMap) {
        if (metadataMap.isEmpty()) {
            return true;
        }
        Iterator<Map.Entry<MetadataIdentifier, Object>> iterable =
                metadataMap.entrySet().iterator();
        while (iterable.hasNext()) {
            Map.Entry<MetadataIdentifier, Object> item = iterable.next();
            if (PROVIDER_SIDE.equals(item.getKey().getSide())) {
                // 提供端的元数据则存储提供端元数据
                this.storeProviderMetadata(item.getKey(), (FullServiceDefinition) item.getValue());
            } else if (CONSUMER_SIDE.equals(item.getKey().getSide())) {
                // 消费端的元数据则存储提供端元数据
                this.storeConsumerMetadata(item.getKey(), (Map) item.getValue());
            }
        }
        return false;
    }

    /**
     * not private. just for unittest.
     */
    void publishAll() {
        logger.info("start to publish all metadata.");
        this.doHandleMetadataCollection(allMetadataReports);
    }

    /**
     * between 2:00 am to 6:00 am, the time is random.
     *
     * @return
     */
    long calculateStartTime() {
        Calendar calendar = Calendar.getInstance();
        long nowMill = calendar.getTimeInMillis();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        long subtract = calendar.getTimeInMillis() + ONE_DAY_IN_MILLISECONDS - nowMill;
        return subtract
                + (FOUR_HOURS_IN_MILLISECONDS / 2)
                + ThreadLocalRandom.current().nextInt(FOUR_HOURS_IN_MILLISECONDS);
    }

    class MetadataReportRetry {
        protected final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(getClass());

        final ScheduledExecutorService retryExecutor =
                Executors.newScheduledThreadPool(0, new NamedThreadFactory("DubboMetadataReportRetryTimer", true));
        volatile ScheduledFuture retryScheduledFuture;
        final AtomicInteger retryCounter = new AtomicInteger(0);
        // retry task schedule period
        long retryPeriod;
        // if no failed report, wait how many times to run retry task.
        int retryTimesIfNonFail = 600;

        int retryLimit;

        public MetadataReportRetry(int retryTimes, int retryPeriod) {
            this.retryPeriod = retryPeriod;
            this.retryLimit = retryTimes;
        }

        void startRetryTask() {
            if (retryScheduledFuture == null) {
                synchronized (retryCounter) {
                    if (retryScheduledFuture == null) {
                        retryScheduledFuture = retryExecutor.scheduleWithFixedDelay(
                                () -> {
                                    // Check and connect to the metadata
                                    try {
                                        int times = retryCounter.incrementAndGet();
                                        logger.info("start to retry task for metadata report. retry times:" + times);
                                        if (retry() && times > retryTimesIfNonFail) {
                                            cancelRetryTask();
                                        }
                                        if (times > retryLimit) {
                                            cancelRetryTask();
                                        }
                                    } catch (Throwable t) { // Defensive fault tolerance
                                        logger.error(
                                                COMMON_UNEXPECTED_EXCEPTION,
                                                "",
                                                "",
                                                "Unexpected error occur at failed retry, cause: " + t.getMessage(),
                                                t);
                                    }
                                },
                                500,
                                retryPeriod,
                                TimeUnit.MILLISECONDS);
                    }
                }
            }
        }

        void cancelRetryTask() {
            if (retryScheduledFuture != null) {
                retryScheduledFuture.cancel(false);
            }
            retryExecutor.shutdown();
        }

        void destroy() {
            cancelRetryTask();
        }

        /**
         * @deprecated only for test
         */
        @Deprecated
        ScheduledExecutorService getRetryExecutor() {
            return retryExecutor;
        }
    }

    private void doSaveSubscriberData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, List<String> urls) {
        if (CollectionUtils.isEmpty(urls)) {
            return;
        }
        List<String> encodedUrlList = new ArrayList<>(urls.size());
        for (String url : urls) {
            encodedUrlList.add(URL.encode(url));
        }
        doSaveSubscriberData(subscriberMetadataIdentifier, encodedUrlList);
    }

    protected abstract void doStoreProviderMetadata(
            MetadataIdentifier providerMetadataIdentifier, String serviceDefinitions);

    protected abstract void doStoreConsumerMetadata(
            MetadataIdentifier consumerMetadataIdentifier, String serviceParameterString);

    protected abstract void doSaveMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url);

    protected abstract void doRemoveMetadata(ServiceMetadataIdentifier metadataIdentifier);

    protected abstract List<String> doGetExportedURLs(ServiceMetadataIdentifier metadataIdentifier);

    protected abstract void doSaveSubscriberData(
            SubscriberMetadataIdentifier subscriberMetadataIdentifier, String urlListStr);

    protected abstract String doGetSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier);

    /**
     * @deprecated only for unit test
     */
    @Deprecated
    protected ExecutorService getReportCacheExecutor() {
        return reportCacheExecutor;
    }

    /**
     * @deprecated only for unit test
     */
    @Deprecated
    protected MetadataReportRetry getMetadataReportRetry() {
        return metadataReportRetry;
    }
}
