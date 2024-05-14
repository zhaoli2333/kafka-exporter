package com.zl.bigdata.kafka.exporter.client;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import okio.Timeout;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Slf4j
public class KafkaAdminClientWapper {

    private AdminClient adminClient;

    public KafkaAdminClientWapper(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public KafkaAdminClientWapper(String user, String password, String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if(user != null) {
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.mechanism", "SCRAM-SHA-256");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + user + "\" password=\"" + password + "\";");
        }
        this.adminClient = AdminClient.create(props);
    }


    public void close() {
        if(this.adminClient != null) {
            this.adminClient.close();
        }
    }


    public Collection<Node> getNodes() {
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        try {
            return describeClusterResult.nodes().get();
        } catch (Exception e) {
            log.error("describeCluster error", e);
            return Collections.EMPTY_LIST;
        }
    }


    public Collection<TopicDescription> getTopics() {
        long start = System.currentTimeMillis();
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Collection<TopicListing> topicListings = null;
        try {
            topicListings = listTopicsResult.listings().get();
        } catch (Exception e) {
            log.error("get topics error", e);
            return Collections.EMPTY_LIST;
        }

        List<String> topics = topicListings.stream().map(TopicListing::name).collect(Collectors.toList());
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topics);
        Map<String, TopicDescription> topicDescriptionMap = null;
        try {
            topicDescriptionMap = describeTopicsResult.all().get();
        } catch (Exception e) {
            log.error("get topics error", e);
            return Collections.EMPTY_LIST;
        }

        return topicDescriptionMap.values();
    }


    public TopicDescription queryTopic(String topicName) {
        try {
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
            Map<String, TopicDescription> descriptionMap = describeTopicsResult.all().get();
            TopicDescription topicDescription = descriptionMap.get(topicName);
            return topicDescription;
        } catch (Exception e) {
            throw new KafkaException(e.getMessage());
        }
    }

    public Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> getLogEndOffset(List<TopicPartition> topicPartitions) {
        List<List<TopicPartition>> parts = Lists.partition(topicPartitions, 1000);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> rst = new HashMap<>();
        parts.stream().forEach(subList -> {
            rst.putAll(getLogEndOffsetByGroup(subList));
        });
        return rst;
    }

    public Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> getLogEndOffsetByGroup(Collection<TopicPartition> topicPartitions) {


        Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpecMap = new HashMap<>();
        for(TopicPartition topicPartition : topicPartitions) {
            topicPartitionOffsetSpecMap.put(topicPartition, OffsetSpec.latest());
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsetsResultInfoMap = null;
        try {
            ListOffsetsResult listOffsetsResult = adminClient.listOffsets(topicPartitionOffsetSpecMap);
            offsetsResultInfoMap = listOffsetsResult.all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("listOffsets failed", e);
            return Collections.EMPTY_MAP;
        }

        return offsetsResultInfoMap;

    }

    public Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> getLogStartOffset(List<TopicPartition> topicPartitions) {
        List<List<TopicPartition>> parts = Lists.partition(topicPartitions, 1000);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> rst = new HashMap<>();
        parts.stream().forEach(subList -> {
            rst.putAll(getLogStartOffsetByGroup(subList));
        });
        return rst;
    }

    public Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> getLogStartOffsetByGroup(Collection<TopicPartition> topicPartitions) {

        Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpecMap = new HashMap<>();
        for(TopicPartition topicPartition : topicPartitions) {
            topicPartitionOffsetSpecMap.put(topicPartition, OffsetSpec.earliest());
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsetsResultInfoMap = null;
        try {
            ListOffsetsResult listOffsetsResult = adminClient.listOffsets(topicPartitionOffsetSpecMap);
            offsetsResultInfoMap = listOffsetsResult.all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("listOffsets failed", e);
            return Collections.EMPTY_MAP;
        }

        return offsetsResultInfoMap;

    }


    public Collection<ConsumerGroupListing> listGroups() {
        ListConsumerGroupsResult listConsumerGroupsResult = adminClient.listConsumerGroups();
        Collection<ConsumerGroupListing> consumerGroupListings = null;
        try {
            consumerGroupListings = listConsumerGroupsResult.all().get();
        } catch (Exception e) {
            log.error("listConsumerGroups failed", e);
            return Collections.EMPTY_LIST;
        }

        return consumerGroupListings;
    }

    public Map<String, ConsumerGroupDescription> describeGroups(List<String> groups) {
        DescribeConsumerGroupsResult describeConsumerGroupsResult = adminClient.describeConsumerGroups(groups);
        try {
            return describeConsumerGroupsResult.all().get();
        } catch (Exception e) {
            log.error("describeGroups failed", e);
            return Collections.EMPTY_MAP;
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> getConsumerOffsets(String group, String topic) {
        ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(group);
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = null;
        try {
            offsetAndMetadataMap = result.partitionsToOffsetAndMetadata().get(3, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.error("listConsumerGroupOffsets timeout, group:{}, topic:{}", group, topic);
            return Collections.EMPTY_MAP;
        } catch (Exception e) {
            log.error("listConsumerGroupOffsets failed", e);
            return Collections.EMPTY_MAP;
        }

        if("*".equals(topic)) {
            return offsetAndMetadataMap;
        }

        Map<TopicPartition, OffsetAndMetadata> rst = new HashMap<>();
        for(Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetAndMetadataMap.entrySet()) {
            if(topic.equals(entry.getKey().topic())) {
                rst.put(entry.getKey(), entry.getValue());
            }
        }

        return rst;

    }

    public Map<TopicPartition, OffsetAndMetadata> getConsumerOffsets(String group, List<TopicPartition> topicPartitions) {
        ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(group, new ListConsumerGroupOffsetsOptions().topicPartitions(topicPartitions));
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = null;
        try {
            offsetAndMetadataMap = result.partitionsToOffsetAndMetadata().get(3, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.error("listConsumerGroupOffsets timeout, group:{}", group);
            return Collections.EMPTY_MAP;
        }catch (Exception e) {
            log.error("listConsumerGroupOffsets failed", e);
            return Collections.EMPTY_MAP;
        }

        return offsetAndMetadataMap;

    }






}
