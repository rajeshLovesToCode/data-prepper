package org.opensearch.dataprepper.plugins.source;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
public class OpenSearchApiCalls implements SearchAPICalls {
    private static final String POINT_IN_TIME_KEEP_ALIVE = "keep_alive";
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchApiCalls.class);
    private static final String KEEP_ALIVE_VALUE = "24h";
    private static final String BATCHSIZEVALUE = "10000";
    @Override
    public   String generatePitId(OpenSearchSourceConfig openSearchSourceConfig, OpenSearchClient client) throws IOException {
        PITRequest pitRequest = new PITRequest(new PITBuilder());
        StringBuilder indexes = Utility.getIndexList(openSearchSourceConfig);
        pitRequest.setIndex(indexes);
        Map<String,String> params = new HashMap<>();
        params.put(POINT_IN_TIME_KEEP_ALIVE,KEEP_ALIVE_VALUE);
        pitRequest.setQueryParameters(params);
        Map  pitResponse =  client._transport().performRequest(pitRequest,PITRequest.ENDPOINT,client._transportOptions());
        LOG.info("PIT Response is : {}  " , pitResponse);
        LOG.info("PIT ID is :  {} "  , pitResponse.get("pit_id"));
        return (String) pitResponse.get("pit_id");
    }
    @Override
    public String searchPitIndexes(String pitId, OpenSearchSourceConfig openSearchSourceConfig, OpenSearchClient client) {
        return "getResponseBody";
    }
    @Override
    public String generateScrollId(OpenSearchSourceConfig openSearchSourceConfig, OpenSearchClient client) {
        ScrollRequest scrollRequest = new ScrollRequest(new ScrollBuilder());
        StringBuilder indexList = Utility.getIndexList(openSearchSourceConfig);
        scrollRequest.setIndex(indexList);
        scrollRequest.setSize(BATCHSIZEVALUE);
        Map<String, Object> response = null;
        try
        {
           response =  client._transport().performRequest(scrollRequest,ScrollRequest.ENDPOINT,client._transportOptions());
           LOG.info("Response is {}  " , response);
        }
       catch (Exception e)
       {
           e.printStackTrace();
       }
        return response.toString();
    }
    @Override
    public String searchScrollIndexes(OpenSearchSourceConfig openSearchSourceConfig, OpenSearchClient client) {

        return "responseBody";
    }
}
