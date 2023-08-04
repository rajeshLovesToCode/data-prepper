package org.opensearch.dataprepper.plugins.kafka.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class TopicsUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TopicsUtils.class);
    private final AdminClient adminClient;


    public TopicsUtils(final KafkaSinkConfig sinkConfig) {
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
        finally {
            adminClient.close();
        }
    }
}
