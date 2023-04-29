package org.opensearch.dataprepper.plugins.source;

import jakarta.json.stream.JsonGenerator;
import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.JsonpSerializable;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;
import java.util.HashMap;
import java.util.Map;
public class PITRequest implements JsonpSerializable {
    private StringBuilder index;
    private String keep_alive;
    private static final String POSTREQUEST = "POST";
    private static  final String PITIDURL =  "/_search/point_in_time";
    public PITRequest(PITBuilder builder) {
        this.index = builder.index;
        this.keep_alive = builder.keep_alive;
    }
    public Map<String,String> queryParameters = new HashMap<>();
    final static JsonpDeserializer<Map> deserializer = new JacksonValueParser<>(Map.class);
    public void setQueryParameters(Map<String, String> queryParameters) {
        this.queryParameters = queryParameters;
    }
    public Map<String, String> getQueryParameters() {
        return queryParameters;
    }
    private Map<String,String> params = new HashMap<>();

    public void setParams(Map<String, String> params) {
        this.params = params;
    }
    public static final Endpoint<PITRequest, Map, ErrorResponse> ENDPOINT =
            new SimpleEndpoint<>(
                    r -> POSTREQUEST,
                    r -> "http://localhost:9200/"+r.getIndex() + PITIDURL,
                    r-> r.getQueryParameters(),
                    SimpleEndpoint.emptyMap(),
                    true,
                    deserializer
            );

    public StringBuilder getIndex() {
        return index;
    }
    public void setIndex(StringBuilder index) {
        this.index = index;
    }
    public String getKeep_alive() {
        return keep_alive;
    }
    public void setKeep_alive(String keep_alive) {
        this.keep_alive = keep_alive;
    }
    @Override
    public void serialize(JsonGenerator generator, JsonpMapper mapper) {
        generator.writeStartObject();
    }
}