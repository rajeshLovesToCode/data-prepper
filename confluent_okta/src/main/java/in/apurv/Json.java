package in.apurv;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Slf4j
public class Json {

    private static Producer<String, SampleRecord> producer;
    private static KafkaConsumer<String, JsonNode> consumer;
    private static final String topic = "json_test_topic";
    private static AdminClient adminClient;


    private static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }


        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);

        return cfg;
    }

    private static void init() throws IOException {
        Properties props = loadConfig("D:\\project_aws\\code_base\\confluent_okta\\src\\main\\resources\\client.properties");
        producer = new KafkaProducer<>(props);
        consumer = new KafkaConsumer<>(props);
        adminClient = AdminClient.create(props);
    }


    public static void main(String[] args) throws Exception {
        init();
       /* String topicName = "manual_topic_30";
        int numPartitions = 1;
        short replicationFactor = 1;
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        adminClient.createTopics(Collections.singletonList(newTopic)).all().get();

        System.out.println("Topic '" + topicName + "' created successfully!");*/


/*        new Thread(() -> {
            IntStream.range(1, 1000).forEach(i -> {
                System.out.println("producing "+ i);
                final SampleRecord sampleRecord = new SampleRecord(i, Double.parseDouble("" + i), "field3-" + i, "field4-" + i);
                sampleRecord.setMessage("testmessage");
                producer.send(new ProducerRecord<String, SampleRecord>
                                (topic,
                                        "key" + i,sampleRecord
                                       )
                        , new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                if(exception!=null){
                                    exception.printStackTrace();
                                }else{
                                    System.out.println(metadata);
                                }
                            }
                        });
                System.out.println("produced "+ i);
            });
            producer.close();
        }).start();*/

        new Thread(() -> {
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, JsonNode> record : records) {
                    System.out.printf("key = %s, value = %s%n", record.key(), record.value());
                }
            }

        }).start();

    }
}