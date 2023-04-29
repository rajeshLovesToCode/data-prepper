package org.opensearch.dataprepper.plugins.source;
import com.fasterxml.jackson.annotation.JsonProperty;
public class SchedulingParameters {
    @JsonProperty("rate")
    private String rate;
    @JsonProperty("job_count")
    private int jobCount;
    @JsonProperty("start_time")
    private String startTime;
    public String getRate() {
        return rate;
    }
    public int getJobCount() {
        return jobCount;
    }
    public String getStartTime() {
        return startTime;
    }
}