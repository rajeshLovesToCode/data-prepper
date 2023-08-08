package org.opensearch.dataprepper.plugins.kafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.CompatibilityCheckResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
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
    private static String CONTENT_TYPE_JSON_VALUE = "application/vnd.schemaregistry.v1+json";

    final int cacheCapacity = 100;

    private final SchemaConfig schemaConfig;

    private final String serdeFormat;

    private final RestService restService;

    private static final String REGISTER_API_PATH = "/subjects/" + "%s" + "/versions?normalize=false";

    private static final String COMPATIBILITY_API_PATH = "compatibility/subjects/" + "%s" + "/versions/" + "%s";

    private final String schemaString;


    public KafkaSinkSchemaUtils(final String serdeFormat, final SchemaConfig schemaConfig) {
        this.serdeFormat = serdeFormat;
        this.schemaConfig = schemaConfig;
        this.schemaRegistryClient = getSchemaRegistryClient();
        this.restService = getRestService();
        this.schemaString=getSchemaString();
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
            if (schemaRegistryClient != null) {
                return schemaRegistryClient.
                        getLatestSchemaMetadata(topic).getSchema();
            }
        } catch (IOException | RestClientException e) {
            LOG.warn(e.getMessage());
        }
        return null;
    }


    public void registerSchema(final String topic) {
        try {
            final String oldSchema = getValueToParse(topic);
            if (ObjectUtils.isEmpty(oldSchema)) {
                register(topic, schemaString);
            } else if (areSchemasDifferent(oldSchema, schemaString) && isSchemasCompatible(schemaString, topic)) {
                register(topic, schemaString);
            }
        } catch (Exception e) {
            throw new RuntimeException("error occured while  schema registeration " + e.getMessage());
        }
    }

    private void register(final String topic, final String schemaString) throws IOException, RestClientException {
        final String path = String.format(REGISTER_API_PATH, topic);
        RegisterSchemaRequest schemaRequest=new RegisterSchemaRequest();
        schemaRequest.setSchema(schemaString);
        schemaRequest.setSchemaType(serdeFormat!=null?serdeFormat.toUpperCase():null);
        final RegisterSchemaResponse registerSchemaResponse = getHttpResponse(schemaRequest.toJson(), path, new TypeReference<>() {
        });
        if (registerSchemaResponse == null) {
            throw new RuntimeException("Schema Registeration failed");
        }
    }


    @NotNull
    private String getSchemaString()  {
        String schemaString = getSchemaDefinition();
        if (schemaString == null) {
            throw new RuntimeException("Invalid schema definition");
        }
        return schemaString;
    }


    private String getSchemaDefinition(){
        try {
            if (schemaConfig.getInlineSchema() != null) {
                return schemaConfig.getInlineSchema();
            } else if (schemaConfig.getSchemaFileLocation() != null) {
                return parseSchemaFromJsonFile(schemaConfig.getSchemaFileLocation());
            } else if (checkS3SchemaValidity(schemaConfig.getS3FileConfig())) {
                return getS3SchemaObject(schemaConfig.getS3FileConfig());
            }
        }
        catch(IOException io){
            LOG.error(io.getMessage());
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

    private CachedSchemaRegistryClient getSchemaRegistryClient() {
        if (schemaConfig != null && schemaConfig.getRegistryURL() != null) {

            return new CachedSchemaRegistryClient(
                    schemaConfig.getRegistryURL(),
                    cacheCapacity, getSchemaProperties());
        }
        return null;
    }

    @NotNull
    private Map getSchemaProperties() {
        Properties schemaProps = new Properties();
        SinkPropertyConfigurer.setSchemaProps(serdeFormat, schemaConfig, schemaProps);
        Map propertiesMap = schemaProps;
        return propertiesMap;
    }

    private Boolean areSchemasDifferent(final String oldSchema, final String newSchema) throws JsonProcessingException {
        return areJsonNodesDifferent(objectMapper.readTree(oldSchema), objectMapper.readTree(newSchema));

    }

    private boolean areJsonNodesDifferent(JsonNode oldNode, JsonNode newNode) {
        if (oldNode.isObject() && newNode.isObject()) {
            ObjectNode objNode1 = (ObjectNode) oldNode;
            ObjectNode objNode2 = (ObjectNode) newNode;
            return !objNode1.equals(objNode2);
        }
        return !oldNode.equals(newNode);
    }


    private <T> T getHttpResponse(final String schemaString, final String path, final TypeReference<T> responseFormat) throws IOException, RestClientException {
        return restService.
                httpRequest(path, method,
                        schemaString.getBytes(StandardCharsets.UTF_8),
                        getRequestProperties(), responseFormat);
    }

    @NotNull
    private Map<String, String> getRequestProperties() {
        Map requestProperties = new HashMap();
        requestProperties.put(CONTENT_TYPE_KEY, CONTENT_TYPE_JSON_VALUE);
        return requestProperties;
    }

    private Boolean isSchemasCompatible(final String schemaString, final String topic) {
        final String path = String.format(COMPATIBILITY_API_PATH, topic, schemaConfig.getVersion());
        try {
            RegisterSchemaRequest request = new RegisterSchemaRequest();
            request.setSchema(schemaString);
            request.setSchemaType(serdeFormat!=null?serdeFormat.toUpperCase():null);
            final CompatibilityCheckResponse compatibilityCheckResponse = getHttpResponse(request.toJson(), path, new TypeReference<>() {
            });
            if (ObjectUtils.isEmpty(compatibilityCheckResponse)) {
                return false;
            }
            return compatibilityCheckResponse.getIsCompatible();
        } catch (Exception ex) {
            LOG.error("Error occured in testing compatiblity " + ex.getMessage());
            return false;
        }
    }

    @NotNull
    private RestService getRestService() {
        final RestService restService = new RestService(schemaConfig.getRegistryURL());
        restService.configure(getSchemaProperties());
        return restService;
    }

}
