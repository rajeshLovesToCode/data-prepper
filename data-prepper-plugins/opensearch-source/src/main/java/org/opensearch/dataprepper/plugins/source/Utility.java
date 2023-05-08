/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import java.util.List;
import java.util.stream.Collectors;
public class Utility {
    public static StringBuilder getIndexList(final OpenSearchSourceConfig openSearchSourceConfig)
    {
        List<String> include = openSearchSourceConfig.getIndexParameters().getInclude();
        List<String> exclude = openSearchSourceConfig.getIndexParameters().getExclude();
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