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

package com.alibaba.nacos.naming.consistency.persistent.raft;

import com.alibaba.nacos.common.utils.IPUtil;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.common.http.Callback;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.notify.EventPublisher;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ValueChangeEvent;
import com.alibaba.nacos.naming.consistency.persistent.ClusterVersionJudgement;
import com.alibaba.nacos.naming.consistency.persistent.PersistentNotifier;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import com.alibaba.nacos.naming.pojo.Record;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

/**
 * Raft core code.
 *
 * @author nacos
 * @deprecated will remove in 1.4.x
 */
@Deprecated
@DependsOn("ProtocolManager")
@Component
public class RaftCore implements Closeable {
    
    public static final String API_VOTE = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/vote";
    
    public static final String API_BEAT = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/beat";
    
    public static final String API_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";
    
    public static final String API_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";
    
    public static final String API_GET = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";
    
    public static final String API_ON_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";
    
    public static final String API_ON_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";
    
    public static final String API_GET_PEER = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/peer";
    
    public static final Lock OPERATE_LOCK = new ReentrantLock();
    
    public static final int PUBLISH_TERM_INCREASE_COUNT = 100;
    
    private volatile ConcurrentMap<String, List<RecordListener>> listeners = new ConcurrentHashMap<>();
    
    private volatile ConcurrentMap<String, Datum> datums = new ConcurrentHashMap<>();
    
    private RaftPeerSet peers;
    
    private final SwitchDomain switchDomain;
    
    private final GlobalConfig globalConfig;
    
    private final RaftProxy raftProxy;
    
    private final RaftStore raftStore;
    
    private final ClusterVersionJudgement versionJudgement;
    
    public final PersistentNotifier notifier;
    
    private final EventPublisher publisher;
    
    private final RaftListener raftListener;
    
    private boolean initialized = false;
    
    private volatile boolean stopWork = false;
    
    private ScheduledFuture masterTask = null;
    
    private ScheduledFuture heartbeatTask = null;
    
    public RaftCore(RaftPeerSet peers, SwitchDomain switchDomain, GlobalConfig globalConfig, RaftProxy raftProxy,
            RaftStore raftStore, ClusterVersionJudgement versionJudgement, RaftListener raftListener) {
        this.peers = peers;
        this.switchDomain = switchDomain;
        this.globalConfig = globalConfig;
        this.raftProxy = raftProxy;
        this.raftStore = raftStore;
        this.versionJudgement = versionJudgement;
        this.notifier = new PersistentNotifier(key -> null == getDatum(key) ? null : getDatum(key).value);
        this.publisher = NotifyCenter.registerToPublisher(ValueChangeEvent.class, 16384);
        this.raftListener = raftListener;
    }
    
    /**
     * Init raft core.
     *
     * @throws Exception any exception during init
     */
    @PostConstruct
    public void init() throws Exception {
        Loggers.RAFT.info("initializing Raft sub-system");
        final long start = System.currentTimeMillis();

        // 将文件中的数据加载到内存中（服务信息，实例列表）
        raftStore.loadDatums(notifier, datums);

        // 将 nacos_home/data/naming/data/meta.properties 文件中的 term 加载到内存中，每个 peer 都有自己的 term，在 nacos 中能不能选上 leader 取决于 term 的大小
        setTerm(NumberUtils.toLong(raftStore.loadMeta().getProperty("term"), 0L));
        
        Loggers.RAFT.info("cache loaded, datum count: {}, current term: {}", datums.size(), peers.getTerm());
        
        initialized = true;
        
        Loggers.RAFT.info("finish to load data from disk, cost: {} ms.", (System.currentTimeMillis() - start));

        // 注册一个选举任务 500ms 执行一次
        masterTask = GlobalExecutor.registerMasterElection(new MasterElection());
        // 注册一个心跳任务 500ms 执行一次
        heartbeatTask = GlobalExecutor.registerHeartbeat(new HeartBeat());

        // 向版本切换注册一个观察者，如果切换了新的版本，就会停止这个 core
        versionJudgement.registerObserver(isAllNewVersion -> {
            stopWork = isAllNewVersion;
            if (stopWork) {
                try {
                    shutdown();
                    raftListener.removeOldRaftMetadata();
                } catch (NacosException e) {
                    throw new NacosRuntimeException(NacosException.SERVER_ERROR, e);
                }
            }
        }, 100);

        // 注册一个监听器，订阅者
        NotifyCenter.registerSubscriber(notifier);
        
        Loggers.RAFT.info("timer started: leader timeout ms: {}, heart-beat timeout ms: {}",
                GlobalExecutor.LEADER_TIMEOUT_MS, GlobalExecutor.HEARTBEAT_INTERVAL_MS);
    }
    
    public Map<String, ConcurrentHashSet<RecordListener>> getListeners() {
        return notifier.getListeners();
    }
    
    /**
     * Signal publish new record. If not leader, signal to leader. If leader, try to commit publish.
     *
     * @param key   key
     * @param value value
     * @throws Exception any exception during publish
     */
    public void signalPublish(String key, Record value) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        if (!isLeader()) {
            ObjectNode params = JacksonUtils.createEmptyJsonNode();
            params.put("key", key);
            params.replace("value", JacksonUtils.transferToJsonNode(value));
            Map<String, String> parameters = new HashMap<>(1);
            parameters.put("key", key);
            
            final RaftPeer leader = getLeader();
            
            raftProxy.proxyPostLarge(leader.ip, API_PUB, params.toString(), parameters);
            return;
        }
        
        OPERATE_LOCK.lock();
        try {
            final long start = System.currentTimeMillis();
            final Datum datum = new Datum();
            datum.key = key;
            datum.value = value;
            if (getDatum(key) == null) {
                datum.timestamp.set(1L);
            } else {
                datum.timestamp.set(getDatum(key).timestamp.incrementAndGet());
            }
            
            ObjectNode json = JacksonUtils.createEmptyJsonNode();
            json.replace("datum", JacksonUtils.transferToJsonNode(datum));
            json.replace("source", JacksonUtils.transferToJsonNode(peers.local()));
            
            onPublish(datum, peers.local());
            
            final String content = json.toString();
            
            final CountDownLatch latch = new CountDownLatch(peers.majorityCount());
            for (final String server : peers.allServersIncludeMyself()) {
                if (isLeader(server)) {
                    latch.countDown();
                    continue;
                }
                final String url = buildUrl(server, API_ON_PUB);
                HttpClient.asyncHttpPostLarge(url, Arrays.asList("key", key), content, new Callback<String>() {
                    @Override
                    public void onReceive(RestResult<String> result) {
                        if (!result.ok()) {
                            Loggers.RAFT
                                    .warn("[RAFT] failed to publish data to peer, datumId={}, peer={}, http code={}",
                                            datum.key, server, result.getCode());
                            return;
                        }
                        latch.countDown();
                    }
                    
                    @Override
                    public void onError(Throwable throwable) {
                        Loggers.RAFT.error("[RAFT] failed to publish data to peer", throwable);
                    }
                    
                    @Override
                    public void onCancel() {
                    
                    }
                });
                
            }
            
            if (!latch.await(UtilsAndCommons.RAFT_PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS)) {
                // only majority servers return success can we consider this update success
                Loggers.RAFT.error("data publish failed, caused failed to notify majority, key={}", key);
                throw new IllegalStateException("data publish failed, caused failed to notify majority, key=" + key);
            }
            
            long end = System.currentTimeMillis();
            Loggers.RAFT.info("signalPublish cost {} ms, key: {}", (end - start), key);
        } finally {
            OPERATE_LOCK.unlock();
        }
    }
    
    /**
     * Signal delete record. If not leader, signal leader delete. If leader, try to commit delete.
     *
     * @param key key
     * @throws Exception any exception during delete
     */
    public void signalDelete(final String key) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        OPERATE_LOCK.lock();
        try {
            
            if (!isLeader()) {
                Map<String, String> params = new HashMap<>(1);
                params.put("key", URLEncoder.encode(key, "UTF-8"));
                raftProxy.proxy(getLeader().ip, API_DEL, params, HttpMethod.DELETE);
                return;
            }
            
            // construct datum:
            Datum datum = new Datum();
            datum.key = key;
            ObjectNode json = JacksonUtils.createEmptyJsonNode();
            json.replace("datum", JacksonUtils.transferToJsonNode(datum));
            json.replace("source", JacksonUtils.transferToJsonNode(peers.local()));
            
            onDelete(datum.key, peers.local());
            
            for (final String server : peers.allServersWithoutMySelf()) {
                String url = buildUrl(server, API_ON_DEL);
                HttpClient.asyncHttpDeleteLarge(url, null, json.toString(), new Callback<String>() {
                    @Override
                    public void onReceive(RestResult<String> result) {
                        if (!result.ok()) {
                            Loggers.RAFT
                                    .warn("[RAFT] failed to delete data from peer, datumId={}, peer={}, http code={}",
                                            key, server, result.getCode());
                            return;
                        }
                        
                        RaftPeer local = peers.local();
                        
                        local.resetLeaderDue();
                    }
                    
                    @Override
                    public void onError(Throwable throwable) {
                        Loggers.RAFT.error("[RAFT] failed to delete data from peer", throwable);
                    }
                    
                    @Override
                    public void onCancel() {
                    
                    }
                });
            }
        } finally {
            OPERATE_LOCK.unlock();
        }
    }
    
    /**
     * Do publish. If leader, commit publish to store. If not leader, stop publish because should signal to leader.
     *
     * @param datum  datum
     * @param source source raft peer
     * @throws Exception any exception during publish
     */
    public void onPublish(Datum datum, RaftPeer source) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        RaftPeer local = peers.local();
        if (datum.value == null) {
            Loggers.RAFT.warn("received empty datum");
            throw new IllegalStateException("received empty datum");
        }
        
        if (!peers.isLeader(source.ip)) {
            Loggers.RAFT
                    .warn("peer {} tried to publish data but wasn't leader, leader: {}", JacksonUtils.toJson(source),
                            JacksonUtils.toJson(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish " + "data but wasn't leader");
        }
        
        if (source.term.get() < local.term.get()) {
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}", JacksonUtils.toJson(source),
                    JacksonUtils.toJson(local));
            throw new IllegalStateException(
                    "out of date publish, pub-term:" + source.term.get() + ", cur-term: " + local.term.get());
        }
        
        local.resetLeaderDue();
        
        // if data should be persisted, usually this is true:
        if (KeyBuilder.matchPersistentKey(datum.key)) {
            raftStore.write(datum);
        }
        
        datums.put(datum.key, datum);
        
        if (isLeader()) {
            local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
        } else {
            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                //set leader term:
                getLeader().term.set(source.term.get());
                local.term.set(getLeader().term.get());
            } else {
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
            }
        }
        raftStore.updateTerm(local.term.get());
        NotifyCenter.publishEvent(ValueChangeEvent.builder().key(datum.key).action(DataOperation.CHANGE).build());
        Loggers.RAFT.info("data added/updated, key={}, term={}", datum.key, local.term);
    }
    
    /**
     * Do delete. If leader, commit delete to store. If not leader, stop delete because should signal to leader.
     *
     * @param datumKey datum key
     * @param source   source raft peer
     * @throws Exception any exception during delete
     */
    public void onDelete(String datumKey, RaftPeer source) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        RaftPeer local = peers.local();
        
        if (!peers.isLeader(source.ip)) {
            Loggers.RAFT
                    .warn("peer {} tried to publish data but wasn't leader, leader: {}", JacksonUtils.toJson(source),
                            JacksonUtils.toJson(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish data but wasn't leader");
        }
        
        if (source.term.get() < local.term.get()) {
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}", JacksonUtils.toJson(source),
                    JacksonUtils.toJson(local));
            throw new IllegalStateException(
                    "out of date publish, pub-term:" + source.term + ", cur-term: " + local.term);
        }
        
        local.resetLeaderDue();
        
        // do apply
        String key = datumKey;
        deleteDatum(key);
        
        if (KeyBuilder.matchServiceMetaKey(key)) {
            
            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                //set leader term:
                getLeader().term.set(source.term.get());
                local.term.set(getLeader().term.get());
            } else {
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
            }
            
            raftStore.updateTerm(local.term.get());
        }
        
        Loggers.RAFT.info("data removed, key={}, term={}", datumKey, local.term);
        
    }
    
    @Override
    public void shutdown() throws NacosException {
        this.stopWork = true;
        this.raftStore.shutdown();
        this.peers.shutdown();
        Loggers.RAFT.warn("start to close old raft protocol!!!");
        Loggers.RAFT.warn("stop old raft protocol task for notifier");
        NotifyCenter.deregisterSubscriber(notifier);
        Loggers.RAFT.warn("stop old raft protocol task for master task");
        masterTask.cancel(true);
        Loggers.RAFT.warn("stop old raft protocol task for heartbeat task");
        heartbeatTask.cancel(true);
        Loggers.RAFT.warn("clean old cache datum for old raft");
        datums.clear();
    }
    
    public class MasterElection implements Runnable {
        
        @Override
        public void run() {
            try {
                // 检查状态
                if (stopWork) {
                    return;
                }
                // 判断是否准备就绪
                if (!peers.isReady()) {
                    return;
                }

                // 获取 RaftPeer, peer 可以理解成节点信息
                RaftPeer local = peers.local();
                // leaderDueMs 减去 500, leaderDueMs 一开始是 0 到 15000 的随机数, 只有减到负数才能继续往下走, 否则直接 return 等待下次调度（也就是等500ms）
                local.leaderDueMs -= GlobalExecutor.TICK_PERIOD_MS;

                // 大于 0 直接返回.
                // 关键点就是 term 上，term 越大越能当选，如果是一开始 term 相等的话，leaderDue 是非常重要的，15000 到 20000 之间的随机数，如果是 15000 的就比 20000 的早执行选举，term 也就早 +1，早发起拉票，早当选。
                if (local.leaderDueMs > 0) {
                    return;
                }
                
                // reset timeout. 重置 leaderDueMs, 15000 + 0~5000 的随机数
                local.resetLeaderDue();
                // 重置 heartbeatDueMs, 5000
                local.resetHeartbeatDue();

                // 发送选票
                sendVote();
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while master election {}", e);
            }
            
        }
        
        private void sendVote() {
            // 获取本机 peer
            RaftPeer local = peers.get(NetUtils.localServer());
            Loggers.RAFT.info("leader timeout, start voting,leader: {}, term: {}", JacksonUtils.toJson(getLeader()),
                    local.term);

            // 将所有的 voteFor 都设置为 null, 这个 voteFor 含义是: 将票投给谁
            peers.reset();

            // 自己的 term 加 1，term 比较重要，如果没有 term 大家都一样，就选不出来了，选择 term 比自己大的节点
            local.term.incrementAndGet();
            // 先投票给自己，将 voteFor 设置成自己的ip
            local.voteFor = local.ip;
            // 转化角色为 candidate  候选人
            local.state = RaftPeer.State.CANDIDATE;

            // 封装参数，将自己的信息发送出去，给自己拉票，将自己这个 peer 作为参数传到集群所有的节点中
            Map<String, String> params = new HashMap<>(1);
            params.put("vote", JacksonUtils.toJson(local));
            // 遍历所有除去自己的 peers 节点
            for (final String server : peers.allServersWithoutMySelf()) {
                final String url = buildUrl(server, API_VOTE);
                try {
                    HttpClient.asyncHttpPost(url, null, params, new Callback<String>() {
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                Loggers.RAFT.error("NACOS-RAFT vote failed: {}, url: {}", result.getCode(), url);
                                return;
                            }

                            // 远端传过来的 peer 节点
                            RaftPeer peer = JacksonUtils.toObj(result.getData(), RaftPeer.class);
                            
                            Loggers.RAFT.info("received approve from peer: {}", JacksonUtils.toJson(peer));

                            // 决定领导者
                            peers.decideLeader(peer);
                            
                        }
                        
                        @Override
                        public void onError(Throwable throwable) {
                            Loggers.RAFT.error("error while sending vote to server: {}", server, throwable);
                        }
                        
                        @Override
                        public void onCancel() {
                        
                        }
                    });
                } catch (Exception e) {
                    Loggers.RAFT.warn("error while sending vote to server: {}", server);
                }
            }
        }
    }
    
    /**
     * Received vote.
     * 处理投票
     *
     * @param remote remote raft peer of vote information
     * @return self-peer information
     */
    public synchronized RaftPeer receivedVote(RaftPeer remote) {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        if (!peers.contains(remote)) {
            throw new IllegalStateException("can not find peer: " + remote.ip);
        }

        // 获取本机 peer
        RaftPeer local = peers.get(NetUtils.localServer());
        // 如果远端 peer 的 term 小于本机的 term 的话
        if (remote.term.get() <= local.term.get()) {
            String msg = "received illegitimate vote" + ", voter-term:" + remote.term + ", votee-term:" + local.term;
            
            Loggers.RAFT.info(msg);
            // 如果 voteFor 为 null 就设置成自己返回
            if (StringUtils.isEmpty(local.voteFor)) {
                local.voteFor = local.ip;
            }
            
            return local;
        }

        // 重置 leaderDue，重置 leaderDue 后选举任务在 15000 到 20000 毫秒之内就不再往下进行了，因为它一次减 500，减不到小于 0 就 return
        local.resetLeaderDue();

        // 改变状态为 follower
        local.state = RaftPeer.State.FOLLOWER;
        // 投票给这个 ip
        local.voteFor = remote.ip;
        // 本地 term 修改为远端的 term
        local.term.set(remote.term.get());
        
        Loggers.RAFT.info("vote {} as leader, term: {}", remote.ip, remote.term);

        // 返回给拉票方
        return local;
    }
    
    public class HeartBeat implements Runnable {
        
        @Override
        public void run() {
            try {
                if (stopWork) {
                    return;
                }
                if (!peers.isReady()) {
                    return;
                }
                
                RaftPeer local = peers.local();
                // heartbeatDue 为 0-5000，减 500 小于 > 0, 直接 return, 直到减到小于 0
                local.heartbeatDueMs -= GlobalExecutor.TICK_PERIOD_MS;
                if (local.heartbeatDueMs > 0) {
                    return;
                }

                // 重置心跳 heartbeatDue，重置成 5000
                local.resetHeartbeatDue();

                // 发送心跳
                sendBeat();
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while sending beat {}", e);
            }
            
        }
        
        private void sendBeat() throws IOException, InterruptedException {
            // 获取本地 peer
            RaftPeer local = peers.local();
            // 如果当前不是集群模式或者自己不是 leader, 则直接 return，只有集群模式的 leader 才能发送心跳
            if (EnvUtil.getStandaloneMode() || local.state != RaftPeer.State.LEADER) {
                return;
            }
            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("[RAFT] send beat with {} keys.", datums.size());
            }

            // 重置 leaderDue. 告诉选举任务，在 15000 到 20000 之间就不要选举了
            local.resetLeaderDue();
            
            // build data
            ObjectNode packet = JacksonUtils.createEmptyJsonNode();
            packet.replace("peer", JacksonUtils.transferToJsonNode(local));
            
            ArrayNode array = JacksonUtils.createEmptyArrayNode();
            
            if (switchDomain.isSendBeatOnly()) {
                Loggers.RAFT.info("[SEND-BEAT-ONLY] {}", switchDomain.isSendBeatOnly());
            }

            // 不仅要发送心跳，还要携带数据，默认返回 false，也就是还得带着数据，包含 key 与 timestamp
            if (!switchDomain.isSendBeatOnly()) {
                for (Datum datum : datums.values()) {
                    
                    ObjectNode element = JacksonUtils.createEmptyJsonNode();

                    if (KeyBuilder.matchServiceMetaKey(datum.key)) {
                        // key 以 com.alibaba.nacos.naming.domains.meta.或 meta. 开头
                        element.put("key", KeyBuilder.briefServiceMetaKey(datum.key));
                    } else if (KeyBuilder.matchInstanceListKey(datum.key)) {
                        // key 以 com.alibaba.nacos.naming.iplist. 或 iplist. 开头
                        element.put("key", KeyBuilder.briefInstanceListkey(datum.key));
                    }
                    element.put("timestamp", datum.timestamp.get());
                    
                    array.add(element);
                }
            }
            
            packet.replace("datums", array);
            // broadcast
            Map<String, String> params = new HashMap<String, String>(1);
            params.put("beat", JacksonUtils.toJson(packet));
            
            String content = JacksonUtils.toJson(params);
            
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
            gzip.close();

            // 压缩数据
            byte[] compressedBytes = out.toByteArray();
            String compressedContent = new String(compressedBytes, StandardCharsets.UTF_8);
            
            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("raw beat data size: {}, size of compressed data: {}", content.length(),
                        compressedContent.length());
            }

            // 向所有的节点发送心跳（抛去自己）发送心跳，收到回应后更新 peer
            for (final String server : peers.allServersWithoutMySelf()) {
                try {
                    final String url = buildUrl(server, API_BEAT);
                    if (Loggers.RAFT.isDebugEnabled()) {
                        Loggers.RAFT.debug("send beat to server " + server);
                    }
                    // 发送心跳请求
                    HttpClient.asyncHttpPostLarge(url, null, compressedBytes, new Callback<String>() {
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                Loggers.RAFT.error("NACOS-RAFT beat failed: {}, peer: {}", result.getCode(), server);
                                MetricsMonitor.getLeaderSendBeatFailedException().increment();
                                return;
                            }
                            
                            peers.update(JacksonUtils.toObj(result.getData(), RaftPeer.class));
                            if (Loggers.RAFT.isDebugEnabled()) {
                                Loggers.RAFT.debug("receive beat response from: {}", url);
                            }
                        }
                        
                        @Override
                        public void onError(Throwable throwable) {
                            Loggers.RAFT.error("NACOS-RAFT error while sending heart-beat to peer: {} {}", server,
                                    throwable);
                            MetricsMonitor.getLeaderSendBeatFailedException().increment();
                        }
                        
                        @Override
                        public void onCancel() {
                        
                        }
                    });
                } catch (Exception e) {
                    Loggers.RAFT.error("error while sending heart-beat to peer: {} {}", server, e);
                    MetricsMonitor.getLeaderSendBeatFailedException().increment();
                }
            }
            
        }
    }
    
    /**
     * Received beat from leader. // TODO split method to multiple smaller method.
     *
     * @param beat beat information from leader
     * @return self-peer information
     * @throws Exception any exception during handle
     */
    public RaftPeer receivedBeat(JsonNode beat) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        // 获取本地 peer
        final RaftPeer local = peers.local();
        // 获取远端 peer 的信息
        final RaftPeer remote = new RaftPeer();
        JsonNode peer = beat.get("peer");
        remote.ip = peer.get("ip").asText();
        remote.state = RaftPeer.State.valueOf(peer.get("state").asText());
        remote.term.set(peer.get("term").asLong());
        remote.heartbeatDueMs = peer.get("heartbeatDueMs").asLong();
        remote.leaderDueMs = peer.get("leaderDueMs").asLong();
        remote.voteFor = peer.get("voteFor").asText();

        // 远端 peer 不是 leader 节点直接抛出异常
        if (remote.state != RaftPeer.State.LEADER) {
            Loggers.RAFT.info("[RAFT] invalid state from master, state: {}, remote peer: {}", remote.state,
                    JacksonUtils.toJson(remote));
            throw new IllegalArgumentException("invalid state from master, state: " + remote.state);
        }

        // 远端 term 小于本地 term 直接抛出异常
        if (local.term.get() > remote.term.get()) {
            Loggers.RAFT
                    .info("[RAFT] out of date beat, beat-from-term: {}, beat-to-term: {}, remote peer: {}, and leaderDueMs: {}",
                            remote.term.get(), local.term.get(), JacksonUtils.toJson(remote), local.leaderDueMs);
            throw new IllegalArgumentException(
                    "out of date beat, beat-from-term: " + remote.term.get() + ", beat-to-term: " + local.term.get());
        }

        // 本地不是 follower 角色，设置为 follower 角色，并把自己的票给对端
        if (local.state != RaftPeer.State.FOLLOWER) {
            
            Loggers.RAFT.info("[RAFT] make remote as leader, remote peer: {}", JacksonUtils.toJson(remote));
            // mk follower
            local.state = RaftPeer.State.FOLLOWER;
            local.voteFor = remote.ip;
        }
        
        final JsonNode beatDatums = beat.get("datums");

        // 重置 leaderDue 与 heartBeatDue，只要 leader 一直不挂，往 follower 持续发送心跳，follower 就一直会重置 leaderDue，follower 的选举任务就一直不会往下走
        // 心跳执行 leader 向其他的节点发送，让其他节点知道 leader 还活着，如果 leader 长时间不发送，follower 就认为 leader 挂了，继续走选举的逻辑，这里重置 leaderDue 是非常重要的。
        local.resetLeaderDue();
        local.resetHeartbeatDue();

        // makeLeader
        peers.makeLeader(remote);

        if (!switchDomain.isSendBeatOnly()) {

            // 收到的 key 集合
            Map<String, Integer> receivedKeysMap = new HashMap<>(datums.size());
            
            for (Map.Entry<String, Datum> entry : datums.entrySet()) {
                receivedKeysMap.put(entry.getKey(), 0);
            }
            
            // now check datums
            List<String> batch = new ArrayList<>();

            // 处理的数量
            int processedCount = 0;
            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT
                        .debug("[RAFT] received beat with {} keys, RaftCore.datums' size is {}, remote server: {}, term: {}, local term: {}",
                                beatDatums.size(), datums.size(), remote.ip, remote.term, local.term);
            }
            for (Object object : beatDatums) {
                processedCount = processedCount + 1;
                
                JsonNode entry = (JsonNode) object;
                String key = entry.get("key").asText();
                final String datumKey;

                if (KeyBuilder.matchServiceMetaKey(key)) {
                    // 以 com.alibaba.nacos.naming.domains.meta. 或 meta. 开头
                    datumKey = KeyBuilder.detailServiceMetaKey(key);
                } else if (KeyBuilder.matchInstanceListKey(key)) {
                    // 以 com.alibaba.nacos.naming.iplist. 或 iplist. 开头
                    datumKey = KeyBuilder.detailInstanceListkey(key);
                } else {
                    // ignore corrupted key:
                    continue;
                }
                
                long timestamp = entry.get("timestamp").asLong();

                // 接收的 key 集合
                receivedKeysMap.put(datumKey, 1);
                
                try {
                    // 本地数据集合包含心跳发送的 key，并且时间大于心跳发送过来的 timestamp 的时间，并且处理的个数小于心跳数据个数，直接跳过
                    if (datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp
                            && processedCount < beatDatums.size()) {
                        continue;
                    }

                    if (!(datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp)) {
                        // 加入处理批次
                        batch.add(datumKey);
                    }

                    // 处理批次小于 50，处理个数小于心跳信息个数，暂不处理
                    if (batch.size() < 50 && processedCount < beatDatums.size()) {
                        continue;
                    }

                    // 所有的要处理批次的 key 值
                    String keys = StringUtils.join(batch, ",");
                    
                    if (batch.size() <= 0) {
                        continue;
                    }
                    
                    Loggers.RAFT.info("get datums from leader: {}, batch size is {}, processedCount is {}"
                                    + ", datums' size is {}, RaftCore.datums' size is {}", getLeader().ip, batch.size(),
                            processedCount, beatDatums.size(), datums.size());
                    
                    // update datum entry
                    String url = buildUrl(remote.ip, API_GET);
                    Map<String, String> queryParam = new HashMap<>(1);
                    queryParam.put("keys", URLEncoder.encode(keys, "UTF-8"));
                    HttpClient.asyncHttpGet(url, null, queryParam, new Callback<String>() {
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                return;
                            }
                            
                            List<JsonNode> datumList = JacksonUtils
                                    .toObj(result.getData(), new TypeReference<List<JsonNode>>() {
                                    });
                            
                            for (JsonNode datumJson : datumList) {
                                Datum newDatum = null;
                                OPERATE_LOCK.lock();
                                try {
                                    
                                    Datum oldDatum = getDatum(datumJson.get("key").asText());
                                    
                                    if (oldDatum != null && datumJson.get("timestamp").asLong() <= oldDatum.timestamp
                                            .get()) {
                                        Loggers.RAFT
                                                .info("[NACOS-RAFT] timestamp is smaller than that of mine, key: {}, remote: {}, local: {}",
                                                        datumJson.get("key").asText(),
                                                        datumJson.get("timestamp").asLong(), oldDatum.timestamp);
                                        continue;
                                    }
                                    
                                    if (KeyBuilder.matchServiceMetaKey(datumJson.get("key").asText())) {
                                        Datum<Service> serviceDatum = new Datum<>();
                                        serviceDatum.key = datumJson.get("key").asText();
                                        serviceDatum.timestamp.set(datumJson.get("timestamp").asLong());
                                        serviceDatum.value = JacksonUtils
                                                .toObj(datumJson.get("value").toString(), Service.class);
                                        newDatum = serviceDatum;
                                    }
                                    
                                    if (KeyBuilder.matchInstanceListKey(datumJson.get("key").asText())) {
                                        Datum<Instances> instancesDatum = new Datum<>();
                                        instancesDatum.key = datumJson.get("key").asText();
                                        instancesDatum.timestamp.set(datumJson.get("timestamp").asLong());
                                        instancesDatum.value = JacksonUtils
                                                .toObj(datumJson.get("value").toString(), Instances.class);
                                        newDatum = instancesDatum;
                                    }
                                    
                                    if (newDatum == null || newDatum.value == null) {
                                        Loggers.RAFT.error("receive null datum: {}", datumJson);
                                        continue;
                                    }
                                    
                                    raftStore.write(newDatum);
                                    
                                    datums.put(newDatum.key, newDatum);
                                    notifier.notify(newDatum.key, DataOperation.CHANGE, newDatum.value);
                                    
                                    local.resetLeaderDue();
                                    
                                    if (local.term.get() + 100 > remote.term.get()) {
                                        getLeader().term.set(remote.term.get());
                                        local.term.set(getLeader().term.get());
                                    } else {
                                        local.term.addAndGet(100);
                                    }
                                    
                                    raftStore.updateTerm(local.term.get());
                                    
                                    Loggers.RAFT.info("data updated, key: {}, timestamp: {}, from {}, local term: {}",
                                            newDatum.key, newDatum.timestamp, JacksonUtils.toJson(remote), local.term);
                                    
                                } catch (Throwable e) {
                                    Loggers.RAFT
                                            .error("[RAFT-BEAT] failed to sync datum from leader, datum: {}", newDatum,
                                                    e);
                                } finally {
                                    OPERATE_LOCK.unlock();
                                }
                            }
                            try {
                                TimeUnit.MILLISECONDS.sleep(200);
                            } catch (InterruptedException e) {
                                Loggers.RAFT.error("[RAFT-BEAT] Interrupted error ", e);
                            }
                            return;
                        }
                        
                        @Override
                        public void onError(Throwable throwable) {
                            Loggers.RAFT.error("[RAFT-BEAT] failed to sync datum from leader", throwable);
                        }
                        
                        @Override
                        public void onCancel() {
                        
                        }
                        
                    });
                    
                    batch.clear();
                    
                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to handle beat entry, key: {}", datumKey);
                }
                
            }
            
            List<String> deadKeys = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : receivedKeysMap.entrySet()) {
                if (entry.getValue() == 0) {
                    deadKeys.add(entry.getKey());
                }
            }
            
            for (String deadKey : deadKeys) {
                try {
                    deleteDatum(deadKey);
                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to remove entry, key={} {}", deadKey, e);
                }
            }
            
        }
        
        return local;
    }
    
    /**
     * Add listener for target key.
     *
     * @param key      key
     * @param listener new listener
     */
    public void listen(String key, RecordListener listener) {
        notifier.registerListener(key, listener);
        
        Loggers.RAFT.info("add listener: {}", key);
        // if data present, notify immediately
        for (Datum datum : datums.values()) {
            if (!listener.interests(datum.key)) {
                continue;
            }
            
            try {
                listener.onChange(datum.key, datum.value);
            } catch (Exception e) {
                Loggers.RAFT.error("NACOS-RAFT failed to notify listener", e);
            }
        }
    }
    
    /**
     * Remove listener for key.
     *
     * @param key      key
     * @param listener listener
     */
    public void unListen(String key, RecordListener listener) {
        notifier.deregisterListener(key, listener);
    }
    
    public void unListenAll(String key) {
        notifier.deregisterAllListener(key);
    }
    
    public void setTerm(long term) {
        peers.setTerm(term);
    }
    
    public boolean isLeader(String ip) {
        return peers.isLeader(ip);
    }
    
    public boolean isLeader() {
        return peers.isLeader(NetUtils.localServer());
    }
    
    /**
     * Build api url.
     *
     * @param ip  ip of api
     * @param api api path
     * @return api url
     */
    public static String buildUrl(String ip, String api) {
        if (!IPUtil.containsPort(ip)) {
            ip = ip + IPUtil.IP_PORT_SPLITER + EnvUtil.getPort();
        }
        return "http://" + ip + EnvUtil.getContextPath() + api;
    }
    
    public Datum<?> getDatum(String key) {
        return datums.get(key);
    }
    
    public RaftPeer getLeader() {
        return peers.getLeader();
    }
    
    public List<RaftPeer> getPeers() {
        return new ArrayList<>(peers.allPeers());
    }
    
    public RaftPeerSet getPeerSet() {
        return peers;
    }
    
    public void setPeerSet(RaftPeerSet peerSet) {
        peers = peerSet;
    }
    
    public int datumSize() {
        return datums.size();
    }
    
    public void addDatum(Datum datum) {
        datums.put(datum.key, datum);
        NotifyCenter.publishEvent(ValueChangeEvent.builder().key(datum.key).action(DataOperation.CHANGE).build());
    }
    
    /**
     * Load datum.
     *
     * @param key datum key
     */
    public void loadDatum(String key) {
        try {
            Datum datum = raftStore.load(key);
            if (datum == null) {
                return;
            }
            datums.put(key, datum);
        } catch (Exception e) {
            Loggers.RAFT.error("load datum failed: " + key, e);
        }
        
    }
    
    private void deleteDatum(String key) {
        Datum deleted;
        try {
            deleted = datums.remove(URLDecoder.decode(key, "UTF-8"));
            if (deleted != null) {
                raftStore.delete(deleted);
                Loggers.RAFT.info("datum deleted, key: {}", key);
            }
            NotifyCenter.publishEvent(
                    ValueChangeEvent.builder().key(URLDecoder.decode(key, "UTF-8")).action(DataOperation.DELETE)
                            .build());
        } catch (UnsupportedEncodingException e) {
            Loggers.RAFT.warn("datum key decode failed: {}", key);
        }
    }
    
    public boolean isInitialized() {
        return initialized || !globalConfig.isDataWarmup();
    }
    
    @Deprecated
    public int getNotifyTaskCount() {
        return (int) publisher.currentEventSize();
    }
    
}
