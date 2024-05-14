package com.zl.bigdata.kafka.exporter.metric;

import com.google.common.collect.Lists;
import com.zl.bigdata.kafka.exporter.client.KafkaAdminClientWapperPool;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Component
@Slf4j
public class KafkaMetricGenerateTask implements Runnable{


    @Autowired
    private KafkaAdminClientWapperPool kafkaAdminClientWapperPool;
    @Autowired
    private KafkaMetricsManagerment kafkaMetricsManagerment;

    private ArrayBlockingQueue<String> pendingGroupQueue;

    private ConcurrentMap<String, Integer> pendingGroupMap;

    private ConcurrentMap<String, Integer> allGroups;

    private ConcurrentMap<String, List<TopicPartition>> groupSubscrptionsMap;

    private volatile boolean isRunning = false;

    private static final int WORK_THREAD_NUM = 10;

    private volatile AtomicBoolean metricAvailable = new AtomicBoolean(false);

    public boolean metricAvailable() {
        return metricAvailable.get();
    }

    @PostConstruct
    public void init() {
        pendingGroupQueue = new ArrayBlockingQueue<>(100000);
        pendingGroupMap = new ConcurrentHashMap<>();
        allGroups = new ConcurrentHashMap<>();
        groupSubscrptionsMap = new ConcurrentHashMap<>();
        isRunning = true;
        for(int i = 0; i < WORK_THREAD_NUM; i++) {
            Thread workThread  = new Thread(this);
            workThread.start();
        }
    }


    @PreDestroy
    public void destory() {
        isRunning = false;
    }


    /**
     * 每2小时一次清空metric，清理已删除的topic和group相关指标
     */
    @Scheduled(fixedRate = 1000 * 3600 * 2, initialDelay = 1000 * 3600 * 2)
    public void clearMetric() {
        kafkaMetricsManagerment.clearConsumergroupLag();
        kafkaMetricsManagerment.clearConsumergroupLagSum();
        kafkaMetricsManagerment.clearConsumergroupCurrentOffset();
        kafkaMetricsManagerment.clearConsumergroupMembers();
    }

    @Scheduled(fixedRate = 30000, initialDelay = 5000)
    public void clusterScheduledTask() {
        Collection<Node> nodes = kafkaAdminClientWapperPool.getClient().getNodes();
        kafkaMetricsManagerment.setClusterBrokers(nodes.size());
        nodes.stream().forEach(node -> {
            kafkaMetricsManagerment.setClusterBrokerInfo(node.idString(), node.host(), 1);
        });
    }


    @Scheduled(fixedRate = 30000, initialDelay = 5000)
    public void topicScheduledTask() {
        log.info("[topicScheduledTask] started ...");
        long start = System.currentTimeMillis();
        // 获取所有topic分区数量
        Collection<TopicDescription> topicDescriptions = kafkaAdminClientWapperPool.getClient().getTopics();
        topicDescriptions.stream().forEach(td -> {
            kafkaMetricsManagerment.setTopicPartitions(td.name(), td.partitions().size());
            if(td.partitions() != null) {
                td.partitions().stream().forEach(topicPartitionInfo -> {
                    kafkaMetricsManagerment.setTopicPartitionLeader(td.name(), topicPartitionInfo.partition(), topicPartitionInfo.leader().id());
                    kafkaMetricsManagerment.setTopicPartitionReplicas(td.name(), topicPartitionInfo.partition(), topicPartitionInfo.replicas().size());
                    kafkaMetricsManagerment.setTopicPartitionReplicas(td.name(), topicPartitionInfo.partition(), topicPartitionInfo.isr().size());
                    if(!CollectionUtils.isEmpty(topicPartitionInfo.isr())) {
                        kafkaMetricsManagerment.setTopicPartitionInSyncReplicas(td.name(), topicPartitionInfo.partition(), topicPartitionInfo.isr().size());
                    }
                    if(topicPartitionInfo.leader() != null
                            && !CollectionUtils.isEmpty(topicPartitionInfo.replicas())
                            && topicPartitionInfo.leader().id() == topicPartitionInfo.replicas().get(0).id()) {
                        kafkaMetricsManagerment.setTopicPartitionUsesPreferredReplica(td.name(), topicPartitionInfo.partition(), 1);
                    } else {
                        kafkaMetricsManagerment.setTopicPartitionUsesPreferredReplica(td.name(), topicPartitionInfo.partition(), 0);
                    }
                    if(!CollectionUtils.isEmpty(topicPartitionInfo.replicas())
                            && !CollectionUtils.isEmpty(topicPartitionInfo.isr())
                            && topicPartitionInfo.isr().size() < topicPartitionInfo.replicas().size()) {
                        kafkaMetricsManagerment.setTopicUnderReplicatedPartition(td.name(), topicPartitionInfo.partition(), 1);
                    } else {
                        kafkaMetricsManagerment.setTopicUnderReplicatedPartition(td.name(), topicPartitionInfo.partition(), 0);
                    }
                });
            }
        });

        List<TopicPartition> topicPartitions = new ArrayList<>();
        for(TopicDescription td : topicDescriptions) {
            for(TopicPartitionInfo tpi : td.partitions()) {
                topicPartitions.add(new TopicPartition(td.name(), tpi.partition()));
            }
        }

        // 获取所有分区的log latest offset
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listStartOffsetsResultInfoMap = kafkaAdminClientWapperPool.getClient().getLogEndOffset(topicPartitions);
        listStartOffsetsResultInfoMap.entrySet().stream().forEach(entry -> {
            kafkaMetricsManagerment.setTopicCurrentOffset(entry.getKey().topic(), entry.getKey().partition(), entry.getValue().offset());
        });

        // 获取所有分区的log start offset
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listEndOffsetsResultInfoMap = kafkaAdminClientWapperPool.getClient().getLogStartOffset(topicPartitions);
        listEndOffsetsResultInfoMap.entrySet().stream().forEach(entry -> {
            kafkaMetricsManagerment.setTopicOldestOffset(entry.getKey().topic(), entry.getKey().partition(), entry.getValue().offset());
        });
        metricAvailable.set(true);
        log.info("[topicScheduledTask] end, cost: {} ms", System.currentTimeMillis() - start);
    }

    @Scheduled(fixedRate = 30000, initialDelay = 10000)
    public void groupConsumeOffsetScheduledTask() {
        log.info("[groupScheduledTask] started ...");
        long start = System.currentTimeMillis();
        Collection<ConsumerGroupListing> consumerGroupListings = kafkaAdminClientWapperPool.getClient().listGroups();
        // update group collection
        Map<String, Integer> groups = new HashMap<>();
        consumerGroupListings.stream().forEach(e -> {
            groups.put(e.groupId(), 1);
        });
        allGroups.clear();
        allGroups.putAll(groups);

        AtomicLong count = new AtomicLong(0);
        AtomicLong requestCount = new AtomicLong(0);
        consumerGroupListings.parallelStream().forEach(cg -> {
            if(!groupSubscrptionsMap.containsKey(cg.groupId())) {
                if(!pendingGroupMap.containsKey(cg.groupId())) {
                    pendingGroupQueue.offer(cg.groupId());
                    pendingGroupMap.put(cg.groupId(), 0);
                    count.incrementAndGet();
                }
                return;
            }
            requestCount.incrementAndGet();
            Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = kafkaAdminClientWapperPool.getClient().getConsumerOffsets(cg.groupId(), groupSubscrptionsMap.get(cg.groupId()));
            processConsumerOffsets(cg.groupId(), offsetAndMetadataMap);
        });

        log.info("[groupScheduledTask] end, cost: {} ms, total group num: {}, push to pendingGroupQueue num: {}, send request to kafka num: {}", System.currentTimeMillis() - start, consumerGroupListings.size(), count.get(), requestCount.get());
    }

    @Scheduled(fixedRate = 120000, initialDelay = 10000)
    public void groupMemebersScheduledTask() {
        log.info("[groupMemebersScheduledTask] started ...");
        long start = System.currentTimeMillis();
        Collection<ConsumerGroupListing> consumerGroupListings = kafkaAdminClientWapperPool.getClient().listGroups();
        List<String> groups = consumerGroupListings.stream().map(e -> e.groupId()).collect(Collectors.toList());

        List<List<String>> parts = Lists.partition(groups, 10);
        parts.parallelStream().forEach(subGroups -> {
            Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap = kafkaAdminClientWapperPool.getClient().describeGroups(subGroups);
            consumerGroupDescriptionMap.entrySet().stream().forEach(entry -> {
                if(entry.getValue() != null && entry.getValue().members() != null) {
                    kafkaMetricsManagerment.setConsumergroupMembers(entry.getKey(), entry.getValue().members().size());
                }
            });
        });

        log.info("[groupMemebersScheduledTask] end, cost: {} ms", System.currentTimeMillis() - start);
    }


    @Scheduled(fixedRate = 600000, initialDelay = 600000)
    public void reputAllGroups() {
        allGroups.keySet().stream().forEach(group -> {
            pendingGroupQueue.offer(group);
        });
        log.info("reput all groups to update subscrpitions, group num:{}", allGroups.size());
    }

    @Scheduled(fixedRate = 5000, initialDelay = 5000)
    public void printPendingQueueInfo() {
        log.info("pendingGroupQueue current size:{}", pendingGroupQueue.size());
    }


    private void processConsumerOffsets(String group, Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap) {
        Map<String, Double> lagSumMap = new HashMap<>();
        offsetAndMetadataMap.entrySet().stream().forEach(entry -> {
            if(entry.getValue() != null && entry.getValue().offset() != -1) {
                String topic = entry.getKey().topic();
                int partition = entry.getKey().partition();
                long consumeOffset = entry.getValue().offset();
                kafkaMetricsManagerment.setConsumerGroupCurrentOffset(group, topic, partition, consumeOffset);
                double partitionLatestOffset = kafkaMetricsManagerment.getTopicCurrentOffset(topic, partition);
                double lag = partitionLatestOffset - consumeOffset;
                if(lag < 0) {
                    lag = 0;
                }
                kafkaMetricsManagerment.setConsumergroupLag(group, topic, partition, lag);
                if(lagSumMap.containsKey(topic)) {
                    lagSumMap.put(topic, lagSumMap.get(topic) + lag);
                } else {
                    lagSumMap.put(topic, lag);
                }
            }
        });

        lagSumMap.entrySet().stream().forEach(stringDoubleEntry -> {
            kafkaMetricsManagerment.setConsumergroupLagSum(group, stringDoubleEntry.getKey(), stringDoubleEntry.getValue());
        });

    }


    @Override
    public void run() {
        log.info("[GroupSubscrptionQueryTask] started");
        while(isRunning) {
            try {
                String group = pendingGroupQueue.poll(5000, TimeUnit.MILLISECONDS);
                if(group == null) {
                    continue;
                }
                pendingGroupMap.remove(group);

                Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = kafkaAdminClientWapperPool.getClient().getConsumerOffsets(group, "*");
                processConsumerOffsets(group, offsetAndMetadataMap);

                List<TopicPartition> topicPartitions = new ArrayList<>();
                offsetAndMetadataMap.entrySet().stream().forEach(entry -> {
                    topicPartitions.add(entry.getKey());
                });
                if(!CollectionUtils.isEmpty(topicPartitions)) {
                    groupSubscrptionsMap.put(group, topicPartitions);
                }

            } catch (Exception e) {
                log.error("GroupSubscrptionQueryTask error", e);
            }
        }
        log.info("[GroupSubscrptionQueryTask] stopped");
    }



}
