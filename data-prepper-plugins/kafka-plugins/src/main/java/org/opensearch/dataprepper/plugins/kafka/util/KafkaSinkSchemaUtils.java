package org.opensearch.dataprepper.plugins.kafka.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaSinkSchemaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkSchemaUtils.class);
    private final CachedSchemaRegistryClient schemaRegistryClient;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static String method = "POST";
    private static String CONTENT_TYPE_KEY = "Content-Type";
    private static String CONTENT_TYPE_JSON_VALUE = "application/json";
    private static String ACCEPT_KEY = "Accept";
    private static String METHOD_HEADER_KEY = "Method";

    public KafkaSinkSchemaUtils(final String serdeFormat, final SchemaConfig schemaConfig) {
        this.schemaRegistryClient = getSchemaRegistryClient(serdeFormat, schemaConfig);
    }

    public Schema getSchema(final String topic) {
        final String valueToParse = getValueToParse(topic);
        if (ObjectUtils.isEmpty(valueToParse)) {
            return null;
        }
        return new Schema.Parser().parse(valueToParse);

    }

    public String getValueToParse(final String topic) {
        try {
            if(schemaRegistryClient!=null) {
                return schemaRegistryClient.
                        getLatestSchemaMetadata(topic + "-value").getSchema();
            }
        } catch (IOException | RestClientException e) {
            LOG.error(e.getMessage());
        }
        return null;
    }


    public void registerSchema(final String topic, final SchemaConfig schemaConfig) {
        try {
            String schemaString = getSchemaString(topic, schemaConfig);
            final RegisterSchemaResponse registerSchemaResponse = register(topic, schemaConfig, schemaString);
            if (registerSchemaResponse == null) {
                throw new RuntimeException("Schema Registeration failed");
            }
            LOG.info("Schema registered Successfully");

        } catch (Exception e) {
            LOG.error("error occured while  schema registeration ");
        }
    }

    private RegisterSchemaResponse register(final String topic, final SchemaConfig schemaConfig, final String schemaString) throws IOException, RestClientException {
        final RestService restService = new RestService(schemaConfig.getRegistryURL());
        final String path = "/subjects/" + topic + "-value/versions";
        TypeReference<RegisterSchemaResponse> REGISTER_RESPONSE_TYPE = new TypeReference<RegisterSchemaResponse>() {
        };
        Map requestProperties = getRequestProperties();

        return restService.
                httpRequest(path, method,
                        schemaString.getBytes(StandardCharsets.UTF_8),
                        requestProperties, REGISTER_RESPONSE_TYPE);
    }

    @NotNull
    private String getSchemaString(final String topic, final SchemaConfig schemaConfig) throws IOException {
        String schemaString = getSchemaDefinition(schemaConfig);
        if (schemaString == null) {
            throw new RuntimeException("Invalid schema definition");
        }
        return schemaString;
    }

    @NotNull
    private Map getRequestProperties() {
        Map requestProperties = new HashMap();
        requestProperties.put(CONTENT_TYPE_KEY, CONTENT_TYPE_JSON_VALUE);
        requestProperties.put(ACCEPT_KEY, CONTENT_TYPE_JSON_VALUE);
        requestProperties.put(METHOD_HEADER_KEY, method);
        return requestProperties;
    }

    public String getSchemaDefinition(final SchemaConfig schemaConfig) throws IOException {
        if (schemaConfig.getInlineSchema() != null) {
            return schemaConfig.getInlineSchema();
        } else if (schemaConfig.getSchemaFileLocation() != null) {
            return parseSchemaFromJsonFile(schemaConfig.getSchemaFileLocation());
        } else if (checkS3SchemaValidity(schemaConfig.getS3FileConfig())) {
            return getS3SchemaObject(schemaConfig.getS3FileConfig());
        }
        return null;
    }


    private boolean checkS3SchemaValidity(final SchemaConfig.S3FileConfig s3FileConfig) throws IOException {
        if (s3FileConfig.getBucketName() != null && s3FileConfig.getFileKey() != null && s3FileConfig.getRegion() != null) {
            return true;
        } else {
            return false;
        }
    }

    private static S3Client buildS3Client(final String region) {
        final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                .addCredentialsProvider(DefaultCredentialsProvider.create()).build();
        return S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(credentialsProvider)
                .httpClientBuilder(ApacheHttpClient.builder())
                .build();
    }

    private static String getS3SchemaObject(final SchemaConfig.S3FileConfig s3FileConfig) throws IOException {
        S3Client s3Client = buildS3Client(s3FileConfig.getRegion());
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3FileConfig.getBucketName())
                .key(s3FileConfig.getFileKey())
                .build();
        ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(getObjectRequest);
        final Map<String, Object> stringObjectMap = objectMapper.readValue(s3Object, new TypeReference<>() {
        });
        return objectMapper.writeValueAsString(stringObjectMap);
    }

    private String parseSchemaFromJsonFile(final String location) throws IOException {
        final Map<?, ?> jsonMap;
        try {
            jsonMap = objectMapper.readValue(Paths.get(location).toFile(), Map.class);
        } catch (FileNotFoundException e) {
            LOG.error("Schema file not found, Error: {}", e.getMessage());
            throw new IOException("Can't proceed without schema.");
        }
        final Map<Object, Object> schemaMap = new HashMap<Object, Object>();
        for (Map.Entry<?, ?> entry : jsonMap.entrySet()) {
            schemaMap.put(entry.getKey(), entry.getValue());
        }
        try {
            return objectMapper.writeValueAsString(schemaMap);
        } catch (Exception e) {
            LOG.error("Unable to parse schema from the provided schema file, Error: {}", e.getMessage());
            throw new IOException("Can't proceed without schema.");
        }
    }

    private CachedSchemaRegistryClient getSchemaRegistryClient(final String serdeFormat, final SchemaConfig schemaConfig) {
        if (schemaConfig != null && schemaConfig.getRegistryURL() != null) {
            return new CachedSchemaRegistryClient(
                    schemaConfig.getRegistryURL(),
                    100, getSchemaProperties(serdeFormat, schemaConfig));
        }
        return null;
    }

    @NotNull
    private Map getSchemaProperties(String serdeFormat, SchemaConfig schemaConfig) {
        Properties schemaProps = new Properties();
        SinkPropertyConfigurer.setSchemaProps(serdeFormat, schemaConfig, schemaProps);
        Map propertiesMap = schemaProps;
        return propertiesMap;
    }
}
