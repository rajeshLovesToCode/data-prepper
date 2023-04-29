package org.opensearch.dataprepper.plugins.source;

public class SourceInfo {
    private String osVersion;
    private String dataSource;
    private Boolean healthStatus = true;
    public String getOsVersion() {
        return osVersion;
    }
    public String getDataSource() {
        return dataSource;
    }
    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }
    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }
    public Boolean getHealthStatus() {
        return healthStatus;
    }
    public void setHealthStatus(Boolean healthStatus) {
        this.healthStatus = healthStatus;
    }
}
