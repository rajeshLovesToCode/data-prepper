package org.opensearch.dataprepper.plugins.kafka.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestClass {

    public static void main(String[] args) throws JsonProcessingException {
        String str="{\n" +
                "    \"schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"MyRecord\\\",\\\"fields\\\":[{\\\"name\\\":\\\"message\\\",\\\"type\\\":\\\"string\\\"}]}\",\n" +
                "    \"references\": [],\n" +
                "    \"schemaType\": \"AVRO\",\n" +
                "    \"name\": \"MySchema\"\n" +
                "}";
        ObjectMapper mapper = new ObjectMapper();
        String s2 = mapper.writeValueAsString(str);
        System.out.println("JSON TO STRING:" + s2);

    }
}
