package com.zl.bigdata.kafka.exporter.config;

import com.zl.bigdata.kafka.exporter.metric.KafkaMetricGenerateTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class ServiceHealthIndicator implements HealthIndicator {

    @Autowired
    private KafkaMetricGenerateTask kafkaMetricGenerateTask;

    @Override
    public Health health() {
        boolean isHealth = kafkaMetricGenerateTask.metricAvailable();
        if(isHealth) {
            return Health.up().build();
        } else {
            return Health.down().build();
        }
    }
}
