package org.opensearch.dataprepper.plugins.source;

import java.util.ArrayList;
import java.util.stream.Collectors;
public class Utility {
    public static StringBuilder getIndexList(OpenSearchSourceConfig openSearchSourceConfig)
    {
        ArrayList<String> include = openSearchSourceConfig.getIndex().getInclude();
        ArrayList<String> exclude = openSearchSourceConfig.getIndex().getExclude();
        String includeIndexes = null;
        String excludeIndexes = null;
        StringBuilder indexList = new StringBuilder();
        if(!include.isEmpty())
            includeIndexes = include.stream().collect(Collectors.joining(","));
        if(!exclude.isEmpty())
            excludeIndexes = exclude.stream().collect(Collectors.joining(",-*"));
        indexList.append(includeIndexes);
        indexList.append(",-*"+excludeIndexes);
        return indexList;
    }

}