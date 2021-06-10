/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.client.naming.core;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.backups.FailoverReactor;
import com.alibaba.nacos.client.naming.beat.BeatInfo;
import com.alibaba.nacos.client.naming.beat.BeatReactor;
import com.alibaba.nacos.client.naming.cache.DiskCache;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.net.NamingProxy;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.util.*;
import java.util.concurrent.*;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Host reactor.
 *
 * @author xuanyin
 */
public class HostReactor implements Closeable {
    
    private static final long DEFAULT_DELAY = 1000L;
    
    private static final long UPDATE_HOLD_INTERVAL = 5000L;
    
    private final Map<String, ScheduledFuture<?>> futureMap = new HashMap<String, ScheduledFuture<?>>();
    
    private final Map<String, ServiceInfo> serviceInfoMap;
    
    private final Map<String, Object> updatingMap;
    
    private final PushReceiver pushReceiver;
    
    private final BeatReactor beatReactor;
    
    private final NamingProxy serverProxy;
    
    private final FailoverReactor failoverReactor;
    
    private final String cacheDir;
    
    private final boolean pushEmptyProtection;
    
    private final ScheduledExecutorService executor;
    
    private final InstancesChangeNotifier notifier;
    
    public HostReactor(NamingProxy serverProxy, BeatReactor beatReactor, String cacheDir) {
        this(serverProxy, beatReactor, cacheDir, false, false, UtilAndComs.DEFAULT_POLLING_THREAD_COUNT);
    }
    
    public HostReactor(NamingProxy serverProxy, BeatReactor beatReactor, String cacheDir, boolean loadCacheAtStart,
            boolean pushEmptyProtection, int pollingThreadCount) {
        // init executorService
        this.executor = new ScheduledThreadPoolExecutor(pollingThreadCount, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("com.alibaba.nacos.client.naming.updater");
                return thread;
            }
        });
        
        this.beatReactor = beatReactor;
        this.serverProxy = serverProxy;
        this.cacheDir = cacheDir;
        if (loadCacheAtStart) {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(DiskCache.read(this.cacheDir));
        } else {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(16);
        }
        this.pushEmptyProtection = pushEmptyProtection;
        this.updatingMap = new ConcurrentHashMap<String, Object>();
        this.failoverReactor = new FailoverReactor(this, cacheDir);
        // 用于接收服务端推送数据
        this.pushReceiver = new PushReceiver(this);
        this.notifier = new InstancesChangeNotifier();

        // 注册事件通知器，PushReceiver 接收到消息后，调用 hostReactor##processServiceJson，如果实例发生变化调用 NotifyCenter.publishEvent 进行事件通知
        NotifyCenter.registerToPublisher(InstancesChangeEvent.class, 16384);
        NotifyCenter.registerSubscriber(notifier);
    }
    
    public Map<String, ServiceInfo> getServiceInfoMap() {
        return serviceInfoMap;
    }
    
    public synchronized ScheduledFuture<?> addTask(UpdateTask task) {
        // 延迟 1s 执行
        return executor.schedule(task, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
    }
    
    /**
     * subscribe instancesChangeEvent.
     *
     * @param serviceName   combineServiceName, such as 'xxx@@xxx'
     * @param clusters      clusters, concat by ','. such as 'xxx,yyy'
     * @param eventListener custom listener
     */
    public void subscribe(String serviceName, String clusters, EventListener eventListener) {
        // 注册监听器
        notifier.registerListener(serviceName, clusters, eventListener);
        // hostReactor 组件获取 serviceInfo 信息
        getServiceInfo(serviceName, clusters);
    }
    
    /**
     * unsubscribe instancesChangeEvent.
     *
     * @param serviceName   combineServiceName, such as 'xxx@@xxx'
     * @param clusters      clusters, concat by ','. such as 'xxx,yyy'
     * @param eventListener custom listener
     */
    public void unSubscribe(String serviceName, String clusters, EventListener eventListener) {
        notifier.deregisterListener(serviceName, clusters, eventListener);
    }
    
    public List<ServiceInfo> getSubscribeServices() {
        return notifier.getSubscribeServices();
    }
    
    /**
     * Process service json. 新的服务实例信息替换本地缓存的服务实例信息，拿新通知的服务实例列表信息与本地缓存的实例列表信息进行比较，看看有没有发生变化，如果有的话，就通知 EventDispatcher 组件
     *
     * @param json service json
     * @return service info
     */
    public ServiceInfo processServiceJson(String json) {
        // 反序列化成 serviceInfo
        ServiceInfo serviceInfo = JacksonUtils.toObj(json, ServiceInfo.class);
        String serviceKey = serviceInfo.getKey();
        if (serviceKey == null) {
            return null;
        }
        // 获取老的 serviceInfo
        ServiceInfo oldService = serviceInfoMap.get(serviceKey);
        
        if (pushEmptyProtection && !serviceInfo.validate()) {
            //empty or error push, just ignore
            return oldService;
        }
        
        boolean changed = false;
        
        if (oldService != null) {
            
            if (oldService.getLastRefTime() > serviceInfo.getLastRefTime()) {
                NAMING_LOGGER.warn("out of date data received, old-t: " + oldService.getLastRefTime() + ", new-t: "
                        + serviceInfo.getLastRefTime());
            }

            // 将新接收的服务实例信息塞入缓存
            serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);
            
            Map<String, Instance> oldHostMap = new HashMap<String, Instance>(oldService.getHosts().size());
            for (Instance host : oldService.getHosts()) {
                oldHostMap.put(host.toInetAddr(), host);
            }
            
            Map<String, Instance> newHostMap = new HashMap<String, Instance>(serviceInfo.getHosts().size());
            for (Instance host : serviceInfo.getHosts()) {
                newHostMap.put(host.toInetAddr(), host);
            }
            
            Set<Instance> modHosts = new HashSet<Instance>();
            Set<Instance> newHosts = new HashSet<Instance>();
            Set<Instance> remvHosts = new HashSet<Instance>();
            
            List<Map.Entry<String, Instance>> newServiceHosts = new ArrayList<Map.Entry<String, Instance>>(
                    newHostMap.entrySet());
            for (Map.Entry<String, Instance> entry : newServiceHosts) {
                Instance host = entry.getValue();
                String key = entry.getKey();
                if (oldHostMap.containsKey(key) && !StringUtils
                        .equals(host.toString(), oldHostMap.get(key).toString())) {
                    // 修改的实例
                    modHosts.add(host);
                    continue;
                }
                
                if (!oldHostMap.containsKey(key)) {
                    // 新的实例
                    newHosts.add(host);
                }
            }
            
            for (Map.Entry<String, Instance> entry : oldHostMap.entrySet()) {
                Instance host = entry.getValue();
                String key = entry.getKey();
                if (newHostMap.containsKey(key)) {
                    continue;
                }

                // 要移除的实例
                if (!newHostMap.containsKey(key)) {
                    remvHosts.add(host);
                }
                
            }
            
            if (newHosts.size() > 0) {
                changed = true;
                NAMING_LOGGER.info("new ips(" + newHosts.size() + ") service: " + serviceInfo.getKey() + " -> "
                        + JacksonUtils.toJson(newHosts));
            }
            
            if (remvHosts.size() > 0) {
                changed = true;
                NAMING_LOGGER.info("removed ips(" + remvHosts.size() + ") service: " + serviceInfo.getKey() + " -> "
                        + JacksonUtils.toJson(remvHosts));
            }
            
            if (modHosts.size() > 0) {
                changed = true;
                updateBeatInfo(modHosts);
                NAMING_LOGGER.info("modified ips(" + modHosts.size() + ") service: " + serviceInfo.getKey() + " -> "
                        + JacksonUtils.toJson(modHosts));
            }
            
            serviceInfo.setJsonFromServer(json);
            
            if (newHosts.size() > 0 || remvHosts.size() > 0 || modHosts.size() > 0) {
                // 从服务端获取实例列表与本地缓存比较如果发生变化，则进行事件通知
                NotifyCenter.publishEvent(new InstancesChangeEvent(serviceInfo.getName(), serviceInfo.getGroupName(),
                        serviceInfo.getClusters(), serviceInfo.getHosts()));
                DiskCache.write(serviceInfo, cacheDir);
            }
            
        } else {
            changed = true;
            NAMING_LOGGER.info("init new ips(" + serviceInfo.ipCount() + ") service: " + serviceInfo.getKey() + " -> "
                    + JacksonUtils.toJson(serviceInfo.getHosts()));
            serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);
            // 服务实例信息发生变化，HostReactor 初始化时注册事件监听器
            NotifyCenter.publishEvent(new InstancesChangeEvent(serviceInfo.getName(), serviceInfo.getGroupName(),
                    serviceInfo.getClusters(), serviceInfo.getHosts()));
            serviceInfo.setJsonFromServer(json);
            DiskCache.write(serviceInfo, cacheDir);
        }
        
        MetricsMonitor.getServiceInfoMapSizeMonitor().set(serviceInfoMap.size());
        
        if (changed) {
            NAMING_LOGGER.info("current ips:(" + serviceInfo.ipCount() + ") service: " + serviceInfo.getKey() + " -> "
                    + JacksonUtils.toJson(serviceInfo.getHosts()));
        }
        
        return serviceInfo;
    }
    
    private void updateBeatInfo(Set<Instance> modHosts) {
        for (Instance instance : modHosts) {
            String key = beatReactor.buildKey(instance.getServiceName(), instance.getIp(), instance.getPort());
            if (beatReactor.dom2Beat.containsKey(key) && instance.isEphemeral()) {
                BeatInfo beatInfo = beatReactor.buildBeatInfo(instance);
                beatReactor.addBeatInfo(instance.getServiceName(), beatInfo);
            }
        }
    }
    
    private ServiceInfo getServiceInfo0(String serviceName, String clusters) {
        // key name@@clusters
        String key = ServiceInfo.getKey(serviceName, clusters);
        
        return serviceInfoMap.get(key);
    }
    
    public ServiceInfo getServiceInfoDirectlyFromServer(final String serviceName, final String clusters)
            throws NacosException {
        // 调用 serverProxy 组件的 queryList() 方法，udp 端口为 0，udp 是给订阅接收通知用的，这里不订阅
        String result = serverProxy.queryList(serviceName, clusters, 0, false);
        if (StringUtils.isNotEmpty(result)) {
            return JacksonUtils.toObj(result, ServiceInfo.class);
        }
        return null;
    }
    
    public ServiceInfo getServiceInfo(final String serviceName, final String clusters) {
        
        NAMING_LOGGER.debug("failover-mode: " + failoverReactor.isFailoverSwitch());
        // 如果 clusters 不为空，则为 name@@clusters
        String key = ServiceInfo.getKey(serviceName, clusters);
        // 判断是否开启了 failover
        if (failoverReactor.isFailoverSwitch()) {
            return failoverReactor.getService(key);
        }

        // 根据 clusters 与 serviceName 生成一个订阅 key，先从本地缓存表中获取 serviceInfo
        ServiceInfo serviceObj = getServiceInfo0(serviceName, clusters);

        if (null == serviceObj) {
            // 如果没有的话，创建 serviceInfo 对象
            serviceObj = new ServiceInfo(serviceName, clusters);

            // 存入本地缓存表中
            serviceInfoMap.put(serviceObj.getKey(), serviceObj);

            // 添加到更新 map 中，表示当前 serviceName 正在更新中
            updatingMap.put(serviceName, new Object());
            // 立即更新服务
            updateServiceNow(serviceName, clusters);
            // 更新结束移除
            updatingMap.remove(serviceName);
            
        } else if (updatingMap.containsKey(serviceName)) {// updatingMap 中存在 serviceName 表示服务正在更新中
            
            if (UPDATE_HOLD_INTERVAL > 0) {
                // hold a moment waiting for update finish
                synchronized (serviceObj) {
                    try {
                        // 等待 5s 更新，如果提前有了数据，会有线程通知然后唤醒这个等待的线程
                        serviceObj.wait(UPDATE_HOLD_INTERVAL);
                    } catch (InterruptedException e) {
                        NAMING_LOGGER
                                .error("[getServiceInfo] serviceName:" + serviceName + ", clusters:" + clusters, e);
                    }
                }
            }
        }
        
        scheduleUpdateIfAbsent(serviceName, clusters);
        
        return serviceInfoMap.get(serviceObj.getKey());
    }
    
    private void updateServiceNow(String serviceName, String clusters) {
        try {
            updateService(serviceName, clusters);
        } catch (NacosException e) {
            NAMING_LOGGER.error("[NA] failed to update serviceName: " + serviceName, e);
        }
    }
    
    /**
     * Schedule update if absent.
     *
     * @param serviceName service name
     * @param clusters    clusters
     */
    public void scheduleUpdateIfAbsent(String serviceName, String clusters) {
        // 从 futureMap 中获取，存在直接返回
        if (futureMap.get(ServiceInfo.getKey(serviceName, clusters)) != null) {
            return;
        }
        
        synchronized (futureMap) {
            if (futureMap.get(ServiceInfo.getKey(serviceName, clusters)) != null) {
                return;
            }

            // 调用 addTask 添加一个 task，返回 future，将 future 缓存到 futureMap 中
            ScheduledFuture<?> future = addTask(new UpdateTask(serviceName, clusters));
            futureMap.put(ServiceInfo.getKey(serviceName, clusters), future);
        }
    }
    
    /**
     * Update service now.
     *
     * @param serviceName service name
     * @param clusters    clusters
     */
    public void updateService(String serviceName, String clusters) throws NacosException {
        // 获取老的 serviceInfo
        ServiceInfo oldService = getServiceInfo0(serviceName, clusters);
        try {
            // 发送请求获取服务实例列表，pushReceiver.getUdpPort()获取一个 udp 端口，pushReceiver 组件用来接收 nacos 服务端推送
            String result = serverProxy.queryList(serviceName, clusters, pushReceiver.getUdpPort(), false);
            // 处理结果
            if (StringUtils.isNotEmpty(result)) {
                processServiceJson(result);
            }
        } finally {
            if (oldService != null) {
                synchronized (oldService) {
                    oldService.notifyAll();
                }
            }
        }
    }
    
    /**
     * Refresh only.
     *
     * @param serviceName service name
     * @param clusters    cluster
     */
    public void refreshOnly(String serviceName, String clusters) {
        try {
            serverProxy.queryList(serviceName, clusters, pushReceiver.getUdpPort(), false);
        } catch (Exception e) {
            NAMING_LOGGER.error("[NA] failed to update serviceName: " + serviceName, e);
        }
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executor, NAMING_LOGGER);
        pushReceiver.shutdown();
        failoverReactor.shutdown();
        NotifyCenter.deregisterSubscriber(notifier);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
    
    public class UpdateTask implements Runnable {
        
        long lastRefTime = Long.MAX_VALUE;
        
        private final String clusters;
        
        private final String serviceName;
        
        /**
         * the fail situation. 1:can't connect to server 2:serviceInfo's hosts is empty
         */
        private int failCount = 0;
        
        public UpdateTask(String serviceName, String clusters) {
            this.serviceName = serviceName;
            this.clusters = clusters;
        }
        
        private void incFailCount() {
            int limit = 6;
            if (failCount == limit) {
                return;
            }
            failCount++;
        }
        
        private void resetFailCount() {
            failCount = 0;
        }
        
        @Override
        public void run() {
            long delayTime = DEFAULT_DELAY;
            
            try {
                // 先从 serviceInfoMap 本地缓存中获取订阅的服务信息 serviceInfo
                ServiceInfo serviceObj = serviceInfoMap.get(ServiceInfo.getKey(serviceName, clusters));

                // 如果是 null 立即更新 service
                if (serviceObj == null) {
                    // 更新服务信息，直接从服务端拉取
                    updateService(serviceName, clusters);
                    return;
                }

                // 如果缓存中 serviceInfo 信息的最后更新时间小于任务里面维护的最后更新时间，说明 serviceInfoMap 缓存中的服务信息过时，调用 updateService 方法拉取
                if (serviceObj.getLastRefTime() <= lastRefTime) {
                    // 更新服务信息
                    updateService(serviceName, clusters);
                    // 获取新的服务信息
                    serviceObj = serviceInfoMap.get(ServiceInfo.getKey(serviceName, clusters));
                } else {
                    // if serviceName already updated by push, we should not override it
                    // since the push data may be different from pull through force push
                    // 刷新，告知服务端要订阅哪个服务
                    refreshOnly(serviceName, clusters);
                }

                // 服务最后更新时间
                lastRefTime = serviceObj.getLastRefTime();

                // 如果 notifier 组件没有订阅这个服务并且 futureMap（任务集合）里面不存在，说明任务被停了，直接 return
                if (!notifier.isSubscribed(serviceName, clusters) && !futureMap
                        .containsKey(ServiceInfo.getKey(serviceName, clusters))) {
                    // abort the update task
                    NAMING_LOGGER.info("update task is stopped, service:" + serviceName + ", clusters:" + clusters);
                    return;
                }

                // 如果 hosts 实例集合是空的，则增加失败次数并返回
                if (CollectionUtils.isEmpty(serviceObj.getHosts())) {
                    incFailCount();
                    return;
                }
                // 该值由服务端决定，如果被订阅了则为10s
                delayTime = serviceObj.getCacheMillis();
                // 重置失败次数
                resetFailCount();
            } catch (Throwable e) {
                // 增加失败次数
                incFailCount();
                NAMING_LOGGER.warn("[NA] failed to update serviceName: " + serviceName, e);
            } finally {
                // 放入调度线程池中执行，正常情况延迟 10s，失败次数多了会延迟，但不会超过60s
                executor.schedule(this, Math.min(delayTime << failCount, DEFAULT_DELAY * 60), TimeUnit.MILLISECONDS);
            }
        }
    }
}
