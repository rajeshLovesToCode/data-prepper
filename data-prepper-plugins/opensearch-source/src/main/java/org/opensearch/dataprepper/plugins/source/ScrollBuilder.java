package org.opensearch.dataprepper.plugins.source;

import org.opensearch.client.util.ObjectBuilder;
import static java.util.Objects.requireNonNull;
public class ScrollBuilder implements ObjectBuilder<ScrollRequest> {
   private StringBuilder index;
    private String scroll;
    private String size;
    public final ScrollBuilder size(StringBuilder v) {
        this.index = v;
        return this;
    }
    public final ScrollBuilder size(String v) {
        this.scroll = v;
        return this;
    }
    public final ScrollBuilder keep_alive(String v) {
        this.size = v;
        return this;
    }
    @Override
    public ScrollRequest build() {
        requireNonNull(this.scroll, "'scroll value' was not set");
        //requireTrue(this.value$isSet, "'value' was not set");
        return new ScrollRequest(this);
    }
}
