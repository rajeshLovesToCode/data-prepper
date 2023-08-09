package org.opensearch.dataprepper.plugins.kafka.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.util.SinkPropertyConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class TopicService {
    private static final Logger LOG = LoggerFactory.getLogger(TopicService.class);
    private final AdminClient adminClient;


    public TopicService(final KafkaSinkConfig sinkConfig) {
        this.adminClient = AdminClient.create(SinkPropertyConfigurer.getPropertiesForAdmintClient(sinkConfig));
    }

    public void createTopic(final String topicName, final Integer numberOfPartitions, final Short replicationFactor) {
        try {
            NewTopic newTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            LOG.info(topicName+" created successfully");

        } catch (Exception e) {
            LOG.info(topicName+" Topic already created so using the existing one");
        }
    }

    public void closeAdminClient()  {
        adminClient.close();
    }
}
