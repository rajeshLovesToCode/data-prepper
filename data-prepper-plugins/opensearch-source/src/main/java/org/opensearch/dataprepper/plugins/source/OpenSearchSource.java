package org.opensearch.dataprepper.plugins.source;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;
@DataPrepperPlugin(name = "opensearch", pluginType = Source.class, pluginConfigurationType =OpenSearchSourceConfig.class)
public class OpenSearchSource implements Source<Record<Event>> {
    private final OpenSearchSourceConfig openSearchSourceConfig;
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSource.class);
    private final PluginMetrics pluginMetrics;
    private OpenSearchClient client;
    private SourceInfoProvider sourceInfoProvider;
    private SourceInfo sourceInfo;
    private List<IndicesRecord> indexInfos;
    @DataPrepperPluginConstructor
    public OpenSearchSource(OpenSearchSourceConfig openSearchSourceConfig, PluginMetrics pluginMetrics) {
        this.openSearchSourceConfig = openSearchSourceConfig;
        this.pluginMetrics = pluginMetrics;
    }
    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        callToApis(openSearchSourceConfig,buffer);
    }
    private void callToApis(OpenSearchSourceConfig openSearchSourceConfig,Buffer<Record<Event>> buffer)  {
        try {
            sourceInfo = new SourceInfo();
            sourceInfoProvider= new SourceInfoProvider();
            String datasource = sourceInfoProvider.getsourceInfo(openSearchSourceConfig);
            sourceInfo.setDataSource(datasource);
            LOG.info("Datasource is : {} " , sourceInfo.getDataSource());
            sourceInfo = sourceInfoProvider.checkStatus(openSearchSourceConfig,sourceInfo);
            if (Boolean.TRUE.equals(sourceInfo.getHealthStatus())) {
                PrepareConnection prepareConnection = new PrepareConnection();
                client = prepareConnection.prepareOpensearchConnection();
                indexInfos =  sourceInfoProvider.callCatIndices(client);
                HashMap<String,String> indexMap = sourceInfoProvider.getIndexMap(indexInfos);
                openSearchSourceConfig.setIndexNames(indexMap);
                LOG.info("Indexes  are {} :  " , indexMap);
                sourceInfoProvider.versionCheck(openSearchSourceConfig,sourceInfo,client,buffer);
            }
            else {
                LOG.info("Retry after sometime");
            }
        }
        catch (Exception e)
        {
            LOG.error("Exception occur : ",e);
        }
    }
    @Override
    public void stop() {

    }
}
