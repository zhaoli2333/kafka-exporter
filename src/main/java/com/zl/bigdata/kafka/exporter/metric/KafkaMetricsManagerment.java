package com.zl.bigdata.kafka.exporter.metric;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;


@Component
@Slf4j
public class KafkaMetricsManagerment {


    @Autowired
    private PrometheusMeterRegistry meterRegistry;

    private Gauge clusterBrokers;

    private Gauge clusterBrokerInfo;

    private Gauge topicPartitions;

    private Gauge topicCurrentOffset;

    private Gauge topicOldestOffset;

    private Gauge topicPartitionLeader;

    private Gauge topicPartitionReplicas;

    private Gauge topicPartitionInSyncReplicas;

    private Gauge topicPartitionUsesPreferredReplica;

    private Gauge topicUnderReplicatedPartition;

    private Gauge consumergroupCurrentOffset;

//    private Gauge consumergroupCurrentOffsetSum;

    private Gauge consumergroupLag;

    private Gauge consumergroupLagSum;

    private Gauge consumergroupMembers;



    @PostConstruct
    public void init() throws Exception {

        CollectorRegistry prometheusRegistry = meterRegistry.getPrometheusRegistry();
        // cluster
        clusterBrokers = Gauge.build().name("kafka_brokers").labelNames().help("Number of Brokers in the Kafka Cluster.").register();
        clusterBrokerInfo = Gauge.build().name("kafka_broker_info").labelNames("id", "address").help("Information about the Kafka Broker.").register();
        // topic
        topicPartitions = Gauge.build().name("kafka_topic_partitions").labelNames("topic").help("Number of partitions for this Topic").register();
        topicCurrentOffset = Gauge.build().name("kafka_topic_partition_current_offset").labelNames("topic", "partition").help("Current Offset of a Broker at Topic/Partition").register();
        topicOldestOffset = Gauge.build().name("kafka_topic_partition_oldest_offset").labelNames("topic","partition").help("Oldest Offset of a Broker at Topic/Partition").register();
        topicPartitionLeader = Gauge.build().name("kafka_topic_partition_leader").labelNames("topic", "partition").help("Leader Broker ID of this Topic/Partition").register();
        topicPartitionReplicas = Gauge.build().name("kafka_topic_partition_replicas").labelNames("topic", "partition").help("Number of Replicas for this Topic/Partition").register();
        topicPartitionInSyncReplicas = Gauge.build().name("kafka_topic_partition_in_sync_replica").labelNames("topic", "partition").help("Number of In-Sync Replicas for this Topic/Partition").register();
        topicPartitionUsesPreferredReplica = Gauge.build().name("kafka_topic_partition_leader_is_preferred").labelNames("topic", "partition").help("1 if Topic/Partition is using the Preferred Broker").register();
        topicUnderReplicatedPartition = Gauge.build().name("kafka_topic_partition_under_replicated_partition").labelNames("topic", "partition").help("1 if Topic/Partition is under Replicated").register();
        // consumer group
        consumergroupCurrentOffset = Gauge.build().name("kafka_consumergroup_current_offset").labelNames("consumergroup", "topic", "partition").help("Current Offset of a ConsumerGroup at Topic/Partition").register();
//        consumergroupCurrentOffsetSum = Gauge.build().name("kafka_consumergroup_current_offset_sum").labelNames("consumergroup", "topic").help("Current Offset of a ConsumerGroup at Topic for all partitions").register();
        consumergroupLag = Gauge.build().name("kafka_consumergroup_lag").labelNames("consumergroup", "topic", "partition").help("Current Approximate Lag of a ConsumerGroup at Topic/Partition").register();
        consumergroupLagSum = Gauge.build().name("kafka_consumergroup_lag_sum").labelNames("consumergroup", "topic").help("Current Approximate Lag of a ConsumerGroup at Topic for all partitions").register();
        consumergroupMembers = Gauge.build().name("kafka_consumergroup_members").labelNames("consumergroup").help("Amount of members in a consumer group").register();

        prometheusRegistry.register(clusterBrokers);
        prometheusRegistry.register(clusterBrokerInfo);
        prometheusRegistry.register(topicPartitions);
        prometheusRegistry.register(topicCurrentOffset);
        prometheusRegistry.register(topicOldestOffset);
        prometheusRegistry.register(topicPartitionLeader);
        prometheusRegistry.register(topicPartitionReplicas);
        prometheusRegistry.register(topicPartitionInSyncReplicas);
        prometheusRegistry.register(topicPartitionUsesPreferredReplica);
        prometheusRegistry.register(topicUnderReplicatedPartition);

//        prometheusRegistry.register(consumergroupCurrentOffsetSum);
        prometheusRegistry.register(consumergroupCurrentOffset);
        prometheusRegistry.register(consumergroupLag);
        prometheusRegistry.register(consumergroupLagSum);
        prometheusRegistry.register(consumergroupMembers);
        log.info("metrics register finished");
    }

//    @PreDestroy
//    public void destory() {
//        clusterBrokers.clear();
//        clusterBrokerInfo.clear();
//        topicPartitions.clear();
//        topicCurrentOffset.clear();
//        topicOldestOffset.clear();
//        topicPartitionLeader.clear();
//        topicPartitionReplicas.clear();
//        topicPartitionInSyncReplicas.clear();
//        topicPartitionUsesPreferredReplica.clear();
//        topicUnderReplicatedPartition.clear();
//        consumergroupCurrentOffset.clear();
//        consumergroupLag.clear();
//        consumergroupLagSum.clear();
//        consumergroupMembers.clear();
//
//    }

    public void clearTopicPartitions() {
        topicPartitions.clear();
    }
    public void clearTopicCurrentOffset() {
        topicCurrentOffset.clear();
    }
    public void clearTopicOldestOffset() {
        topicOldestOffset.clear();
    }
    public void clearConsumergroupCurrentOffset() {
        consumergroupCurrentOffset.clear();
    }
    public void clearConsumergroupLag() {
        consumergroupLag.clear();
    }
    public void clearConsumergroupLagSum() {
        consumergroupLagSum.clear();
    }
    public void clearConsumergroupMembers() {
        consumergroupMembers.clear();
    }



    public synchronized void setClusterBrokers(double value) {
        clusterBrokers.set(value);
    }

    public synchronized void setClusterBrokerInfo(String id, String address, double value) {
        clusterBrokerInfo.labels(id, address).set(value);
    }

    public synchronized void setTopicPartitions(String topic, double value) {
        topicPartitions.labels(topic).set(value);
    }

    public synchronized void setTopicCurrentOffset(String topic, int partition, double value) {
        topicCurrentOffset.labels(topic, Integer.toString(partition)).set(value);
    }

    public synchronized double getTopicCurrentOffset(String topic, int partition) {
        return topicCurrentOffset.labels(topic, Integer.toString(partition)).get();
    }

    public synchronized void setTopicOldestOffset(String topic, int partition, double value) {
        topicOldestOffset.labels(topic, Integer.toString(partition)).set(value);
    }

//    public double getTopicOldestOffset(String topic, int partition) {
//        return topicOldestOffset.labels(topic, Integer.toString(partition)).get();
//    }

    public synchronized void setTopicPartitionLeader(String topic, int partition, double value) {
        topicPartitionLeader.labels(topic, Integer.toString(partition)).set(value);
    }

    public synchronized void setTopicPartitionReplicas(String topic, int partition, double value) {
        topicPartitionReplicas.labels(topic, Integer.toString(partition)).set(value);
    }

    public synchronized void setTopicPartitionInSyncReplicas(String topic, int partition, double value) {
        topicPartitionInSyncReplicas.labels(topic, Integer.toString(partition)).set(value);
    }

    public synchronized void setTopicPartitionUsesPreferredReplica(String topic, int partition, double value) {
        topicPartitionUsesPreferredReplica.labels(topic, Integer.toString(partition)).set(value);
    }

    public synchronized void setTopicUnderReplicatedPartition(String topic, int partition, double value) {
        topicUnderReplicatedPartition.labels(topic, Integer.toString(partition)).set(value);
    }

    public synchronized void setConsumerGroupCurrentOffset(String group, String topic, int partition, double value) {
        consumergroupCurrentOffset.labels(group, topic, Integer.toString(partition)).set(value);
    }


//    public void labelConsumergroupCurrentOffsetSum(String group, String topic, double value) {
//        consumergroupCurrentOffsetSum.labels(group, topic).set(value);
//    }

    public synchronized void setConsumergroupLag(String group, String topic, int partition, double value) {
        consumergroupLag.labels(group, topic, Integer.toString(partition)).set(value);
    }

    public synchronized void setConsumergroupLagSum(String group, String topic, double value) {
        consumergroupLagSum.labels(group, topic).set(value);
    }

    public synchronized void setConsumergroupMembers(String group, double value) {
        consumergroupMembers.labels(group).set(value);
    }

}
