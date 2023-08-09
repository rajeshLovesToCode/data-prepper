package org.opensearch.dataprepper.plugins.kafka.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TestClass {

    public static void main(String[] args) throws JsonProcessingException {


        String json1="{\n" +
                "  \"type\": \"object\",\n" +
                "  \"properties\": {\n" +
                "    \"Year\": {\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    \"Age\": {\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    \"Ethnic\": {\n" +
                "      \"type\": \"string\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String json2 = "{\n" +
                "    \"schema\": \"{\\\"type\\\":\\\"Object\\\",\\\"name\\\":\\\"MyRecord\\\",\\\"fields\\\":[{\\\"name\\\":\\\"message\\\",\\\"type\\\":\\\"string\\\"}]}\",\n" +
                "    \"references\": [],\n" +
                "    \"schemaType\": \"AVRO\",\n" +
                "    \"name\": \"MySchema\"\n" +
                "}";
        ObjectMapper objectMapper = new ObjectMapper();

        isSchemaDifferent(json1, json2, objectMapper);

    }

    private static void isSchemaDifferent(String json1, String json2, ObjectMapper objectMapper) {
        try {
            JsonNode node1 = objectMapper.readTree(json1);
            JsonNode node2 = objectMapper.readTree(json2);
            if (areJsonNodesEqual(node1, node2)) {
                System.out.println("JSON nodes are equal.");
            } else {
                System.out.println("JSON nodes are not equal.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean areJsonNodesEqual(JsonNode node1, JsonNode node2) {
        // If both are null, consider them equal
        if (node1 == null && node2 == null) {
            return true;
        }

        // If either one is null, they are not equal
        if (node1 == null || node2 == null) {
            return false;
        }

        // Check if both are objects
        if (node1.isObject() && node2.isObject()) {
            ObjectNode objNode1 = (ObjectNode) node1;
            ObjectNode objNode2 = (ObjectNode) node2;

            // Compare their field names and values
            return objNode1.equals(objNode2);
        }

        // Compare other types of nodes
        return node1.equals(node2);
    }
}
