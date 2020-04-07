package im.redpanda.flaschenpost;

import im.redpanda.core.KademliaId;

import java.util.Objects;

@Deprecated
public class FPKey {


    KademliaId id;
    long timestamp;


    public FPKey(KademliaId id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    public KademliaId getId() {
        return id;
    }

    public void setId(KademliaId id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FPKey fpKey = (FPKey) o;
        return timestamp == fpKey.timestamp &&
//                contentHash == fpKey.contentHash &&
                Objects.equals(id, fpKey.id);
    }


    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp);
    }
}
