package im.redpanda.kademlia;


import im.redpanda.core.KademliaId;
import im.redpanda.core.NodeId;
import im.redpanda.crypt.Sha256Hash;
import im.redpanda.crypt.Utils;
import io.sentry.Sentry;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.interfaces.ECKey;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class KadContent {

    public static final String pattern = "dd.MM.yy";

    private KademliaId id; //we store the ID duplicated because of performance reasons (new lookup in the hashmap costs more than a bit of memory)
    private long timestamp; //created at (or updated)
    private byte[] pubkey;
    private byte[] content;
    private byte[] signature;


    /**
     * Creates a new KadContent object, please note that the KademliaId will be computed from the timestmap and
     * public key to ensure the integrity of the KademliaId and timestamp.
     *
     * @param timestamp
     * @param pubkey
     * @param content
     */
    public KadContent(long timestamp, byte[] pubkey, byte[] content) {
        this.timestamp = timestamp;
        this.pubkey = pubkey;
        this.content = content;
    }

    /**
     * Creates a new KadContent object, please note that the KademliaId will be computed from the timestmap and
     * public key to ensure the integrity of the KademliaId and timestamp.
     *
     * @param timestamp
     * @param pubkey
     * @param content
     * @param signature
     */
    public KadContent(long timestamp, byte[] pubkey, byte[] content, byte[] signature) {
        this.timestamp = timestamp;
        this.pubkey = pubkey;
        this.content = content;
        this.signature = signature;
    }

    /**
     * Creates a new KadContent object, please note that the KademliaId will be computed from the timestmap and
     * public key to ensure the integrity of the KademliaId and timestamp.
     *
     * @param pubkey
     * @param content
     */
    public KadContent(byte[] pubkey, byte[] content) {
        this.timestamp = System.currentTimeMillis();
        this.pubkey = pubkey;
        this.content = content;
    }


    public byte[] getPubkey() {
        return pubkey;
    }

    public KademliaId getId() {
        if (id == null) {
            id = createKademliaId(timestamp, pubkey);
        }

        return id;
    }

    public static KademliaId createKademliaId(long timestamp, byte[] pubkey) {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            //todo lets check if this is the corret time zone for the dart code as well...
            String date = simpleDateFormat.format(new Date(timestamp));

//            System.out.println("kadcontent date: " + date);

            byte[] dateBytes = date.getBytes("UTF-8");

            ByteBuffer byteBuffer = ByteBuffer.allocate(dateBytes.length + pubkey.length);
            byteBuffer.put(dateBytes);
            byteBuffer.put(pubkey);

            byte[] sha256 = Sha256Hash.create(byteBuffer.array()).getBytes();
            return KademliaId.fromFirstBytes(sha256);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            Sentry.capture(e);
        }
        return null;
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

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public byte[] getSignature() {
        if (signature == null) {
            throw new RuntimeException("this content was not signed, signature is null!");
        }
        return signature;
    }

    public Sha256Hash createHash() {

        ByteBuffer buffer = ByteBuffer.allocate(8 + content.length);
        buffer.putLong(timestamp);
        buffer.put(content);

        Sha256Hash hash = Sha256Hash.create(buffer.array());
        return hash;
    }


    public void signWith(NodeId nodeId) {
        Sha256Hash hash = createHash();

        signature = nodeId.sign(hash.getBytes());
    }

    public boolean verify() {
        Sha256Hash hash = createHash();

        NodeId pubNodId = NodeId.importPublic(pubkey);

        return pubNodId.verify(hash.getBytes(), getSignature());
    }
}
