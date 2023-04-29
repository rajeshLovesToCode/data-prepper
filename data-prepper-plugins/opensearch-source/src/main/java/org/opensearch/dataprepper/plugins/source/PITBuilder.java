package org.opensearch.dataprepper.plugins.source;

import org.opensearch.client.util.ObjectBuilder;
import static java.util.Objects.requireNonNull;
public  class PITBuilder implements ObjectBuilder<PITRequest> {
    protected StringBuilder index;
    protected String keep_alive;
    public final PITBuilder index(StringBuilder v) {
        this.index = v;
        return this;
    }
    public final PITBuilder keep_alive(String v) {
        this.keep_alive = v;
        return this;
    }
    public PITRequest build() {
        requireNonNull(this.index, "'index' was not set");
        //requireTrue(this.value$isSet, "'value' was not set");
        return new PITRequest(this);
    }
}