package com.zl.bigdata.kafka.exporter.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Component
public class KafkaAdminClientWapperPool {

    @Value("${kafka.cluster.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${kafka.cluster.user}")
    private String user;
    @Value("${kafka.cluster.password}")
    private String password;
    private static final Integer CLIENT_CNT = 5;

    private final List<KafkaAdminClientWapper> kafkaClients = new ArrayList<>(CLIENT_CNT);


    @PostConstruct
    public void init() {
        for(int i = 0; i < CLIENT_CNT; i++) {
            kafkaClients.add(new KafkaAdminClientWapper(user, password, bootstrapServers));
        }
    }

    public KafkaAdminClientWapper getClient() throws KafkaException {

        return kafkaClients.get(ThreadLocalRandom.current().nextInt(CLIENT_CNT));

    }


    @PreDestroy
    public void destory() {
        for(KafkaAdminClientWapper kafkaAdminClientWapper : kafkaClients) {
            kafkaAdminClientWapper.close();
        }
    }




}
