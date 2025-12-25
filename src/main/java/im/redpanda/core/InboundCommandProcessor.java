package im.redpanda.core;

import im.redpanda.crypt.Utils;
import im.redpanda.flaschenpost.GMContent;
import im.redpanda.flaschenpost.GMParser;
import im.redpanda.jobs.Job;
import im.redpanda.jobs.KademliaInsertJob;
import im.redpanda.jobs.KademliaSearchJob;
import im.redpanda.jobs.KademliaSearchJobAnswerPeer;
import im.redpanda.kademlia.KadContent;
import com.google.protobuf.InvalidProtocolBufferException;
import im.redpanda.proto.*;
import static com.google.protobuf.ByteString.copyFrom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;

/**
 * Parses and processes inbound commands for a peer connection.
 * Extracted from ConnectionReaderThread to improve SRP and testability.
 */
public class InboundCommandProcessor {
    private static final Logger logger = LogManager.getLogger();
    public static final int MIN_SIGNATURE_LEN = 70;

    private final ServerContext serverContext;

    public InboundCommandProcessor(ServerContext serverContext) {
        this.serverContext = serverContext;
    }

    public void loopCommands(Peer peer, ByteBuffer readBuffer) {
        readBuffer.flip();

        int parsedBytesLocally = -1;

        while (readBuffer.hasRemaining() && parsedBytesLocally != 0 && peer.isConnected()) {
            int newPosition = readBuffer.position();
            byte b = readBuffer.get();
            Log.put("command: " + b + " " + readBuffer, 200);
            parsedBytesLocally = parseCommand(b, readBuffer, peer);
            if (!peer.isConnected()) {
                return;
            }
            peer.lastCommand = b;
            newPosition += parsedBytesLocally;
            readBuffer.position(newPosition);
        }

        readBuffer.compact();
    }

    public int parseCommand(byte command, ByteBuffer readBuffer, Peer peer) {
        if (command == Command.PING) {
            return handlePing(peer);
        }
        if (command == Command.PONG) {
            return handlePong(peer);
        }
        if (command == Command.REQUEST_PEERLIST) {
            return handleRequestPeerList(peer);
        }

        // Commands with payload require reading length first
        byte[] payload = null;
        if (isPayloadCommand(command)) {
            payload = readMessage(readBuffer);
            if (payload == null) {
                return 0; // Not enough data yet
            }
        }

        try {
            switch (command) {
                case Command.SEND_PEERLIST:
                    return handleSendPeerList(payload, peer) + 4 + payload.length;
                case Command.UPDATE_REQUEST_TIMESTAMP:
                    return handleUpdateRequestTimestamp(peer);
                case Command.UPDATE_ANSWER_TIMESTAMP:
                    return handleUpdateAnswerTimestamp(readBuffer, peer); // Special case: reads Long directly
                case Command.UPDATE_REQUEST_CONTENT:
                    return handleUpdateRequestContent(peer);
                case Command.UPDATE_ANSWER_CONTENT:
                    return handleUpdateAnswerContent(readBuffer, peer); // Special case: complex read
                case Command.ANDROID_UPDATE_REQUEST_TIMESTAMP:
                    return handleAndroidUpdateRequestTimestamp(peer);
                case Command.ANDROID_UPDATE_ANSWER_TIMESTAMP:
                    return handleAndroidUpdateAnswerTimestamp(readBuffer, peer); // Special case: reads Long directly
                case Command.ANDROID_UPDATE_REQUEST_CONTENT:
                    return handleAndroidUpdateRequestContent(peer);
                case Command.ANDROID_UPDATE_ANSWER_CONTENT:
                    return handleAndroidUpdateAnswerContent(readBuffer, peer); // Special case: complex read
                case Command.JOB_ACK:
                    handleJobAck(payload, peer);
                    return 1 + 4 + payload.length;
                case Command.KADEMLIA_GET:
                    handleKademliaGet(payload, peer);
                    return 1 + 4 + payload.length;
                case Command.KADEMLIA_STORE:
                    handleKademliaStore(payload, peer);
                    return 1 + 4 + payload.length;
                case Command.KADEMLIA_GET_ANSWER:
                    handleKademliaGetAnswer(payload, peer);
                    return 1 + 4 + payload.length;
                case Command.FLASCHENPOST_PUT:
                    handleFlaschenpostPut(payload, peer);
                    return 1 + 4 + payload.length;
                default:
                    throw new RuntimeException(
                            "Got unknown command from peer: " + command + " last cmd: " + peer.lastCommand
                                    + " lightClient: " + peer.isLightClient());
            }
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to parse protobuf for command " + command, e);
            return 1 + 4 + payload.length; // Skip the malformed message
        }
    }

    private boolean isPayloadCommand(byte command) {
        return command == Command.SEND_PEERLIST ||
                command == Command.JOB_ACK ||
                command == Command.KADEMLIA_GET ||
                command == Command.KADEMLIA_STORE ||
                command == Command.KADEMLIA_GET_ANSWER ||
                command == Command.FLASCHENPOST_PUT;
    }

    private byte[] readMessage(ByteBuffer readBuffer) {
        if (readBuffer.remaining() < 4) {
            return null;
        }
        readBuffer.mark();
        int length = readBuffer.getInt();
        if (readBuffer.remaining() < length) {
            readBuffer.reset();
            return null;
        }
        byte[] bytes = new byte[length];
        readBuffer.get(bytes);
        return bytes;
    }

    private int handlePing(Peer peer) {
        Log.put("Received ping command", 200);
        if (!serverContext.getPeerList().contains(peer.getKademliaId())) {
            logger.error(String.format("Got PING from node not in our peerlist, lets add it.... %s, id: %s", peer,
                    peer.getKademliaId()));
            serverContext.getPeerList().add(peer);
            return 0;
        }
        peer.getWriteBufferLock().lock();
        try {
            peer.writeBuffer.put(Command.PONG);
        } finally {
            peer.getWriteBufferLock().unlock();
        }
        return 1;
    }

    private int handlePong(Peer peer) {
        Log.put("Received pong command", 200);
        peer.ping = (1 * peer.ping + (double) (System.currentTimeMillis() - peer.lastPinged)) / 2;
        peer.setLastPongReceived(System.currentTimeMillis());
        return 1;
    }

    private int handleRequestPeerList(Peer peer) {
        serverContext.getPeerList().getReadWriteLock().readLock().lock();
        try {
            var builder = SendPeerList.newBuilder();
            for (Peer peerToCheck : serverContext.getPeerList().getPeerArrayList()) {
                var peerBuilder = PeerInfoProto.newBuilder()
                        .setIp(peerToCheck.ip)
                        .setPort(peerToCheck.getPort());
                if (peerToCheck.getNodeId() != null && peerToCheck.getNodeId().hasKey()) {
                    peerBuilder.setNodeId(NodeIdProto.newBuilder()
                            .setPublicKeyBytes(
                                    copyFrom(peerToCheck.getNodeId().exportPublic()))
                            .build());
                }
                builder.addPeers(peerBuilder.build());
            }
            byte[] data = builder.build().toByteArray();
            peer.getWriteBufferLock().lock();
            try {
                peer.writeBuffer.put(Command.SEND_PEERLIST);
                peer.writeBuffer.putInt(data.length);
                peer.writeBuffer.put(data);
                peer.setWriteBufferFilled();
            } finally {
                peer.getWriteBufferLock().unlock();
            }
        } finally {
            serverContext.getPeerList().getReadWriteLock().readLock().unlock();
        }
        return 1;
    }

    private int handleSendPeerList(byte[] bytesForPeerList, Peer peer) throws InvalidProtocolBufferException {
        SendPeerList sendPeerList = SendPeerList.parseFrom(bytesForPeerList);
        for (PeerInfoProto peerProto : sendPeerList.getPeersList()) {
            NodeId nodeId = null;
            if (peerProto.hasNodeId()) {
                nodeId = NodeId.importPublic(peerProto.getNodeId().getPublicKeyBytes().toByteArray());
            }
            String ip = peerProto.getIp();
            int port = peerProto.getPort();
            if (nodeId != null) {
                if (nodeId.getKademliaId().equals(serverContext.getNonce())) {
                    Log.put("found ourselves in the peerlist", 80);
                    continue;
                }
                if (ip == null) {
                    System.out.println("found a peer with ip null...");
                    continue;
                }
                Peer newPeer = new Peer(ip, port, nodeId);
                var byKademliaId = Node.getByKademliaId(serverContext, nodeId.getKademliaId());
                if (byKademliaId != null) {
                    byKademliaId.addConnectionPoint(ip, port);
                } else {
                    new Node(serverContext, nodeId);
                }
                serverContext.getPeerList().add(newPeer);
            } else {
                serverContext.getPeerList().add(new Peer(ip, port));
            }
        }
        return 1; // Base command length, payload length added by caller
    }

    private int handleUpdateRequestTimestamp(Peer peer) {
        ByteBuffer writeBuffer = peer.getWriteBuffer();
        peer.writeBufferLock.lock();
        try {
            writeBuffer.put(Command.UPDATE_ANSWER_TIMESTAMP);
            writeBuffer.putLong(serverContext.getLocalSettings().getUpdateTimestamp());
        } finally {
            peer.writeBufferLock.unlock();
        }
        peer.setWriteBufferFilled();
        return 1;
    }

    private int handleUpdateAnswerTimestamp(ByteBuffer readBuffer, Peer peer) {
        if (8 > readBuffer.remaining()) {
            return 0;
        }
        long othersTimestamp = readBuffer.getLong();
        if (othersTimestamp < serverContext.getLocalSettings().getUpdateTimestamp()) {
            System.out.println("WARNING: peer has outdated redPandaj version! " + peer.getNodeId());
        }
        if (othersTimestamp > serverContext.getLocalSettings().getUpdateTimestamp() && Settings.isLoadUpdates()) {
            Runnable runnable = () -> {
                ConnectionReaderThread.updateDownloadLock.lock();
                try {
                    System.out.println("our version is outdated, we try to download it from this peer!");
                    peer.writeBufferLock.lock();
                    peer.getWriteBuffer().put(Command.UPDATE_REQUEST_CONTENT);
                    peer.writeBufferLock.unlock();
                    peer.setWriteBufferFilled();
                    try {
                        Thread.sleep(60000);
                    } catch (InterruptedException ignored) {
                    }
                } finally {
                    System.out.println("we can now download it from another peer...");
                    ConnectionReaderThread.updateDownloadLock.unlock();
                }
            };
            Server.threadPool.submit(runnable);
        }
        return 1 + 8;
    }

    private int handleUpdateRequestContent(Peer peer) {
        if (serverContext.getLocalSettings().getUpdateTimestamp() == -1) {
            return 1;
        }
        if (serverContext.getLocalSettings().getUpdateSignature() == null) {
            System.out.println("we dont have an official signature to upload that update to other peers!");
            return 1;
        }
        Runnable runnable = () -> {
            ConnectionReaderThread.updateUploadLock.acquireUninterruptibly();
            try {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {
                }
                Path path = Settings.isSeedNode() ? Paths.get("target/redpanda.jar") : Paths.get("redpanda.jar");
                try {
                    System.out.println("we send the update to a peer!");
                    byte[] data = Files.readAllBytes(path);
                    ByteBuffer a = ByteBuffer.allocate(
                            1 + 8 + 4 + serverContext.getLocalSettings().getUpdateSignature().length + data.length);
                    a.put(Command.UPDATE_ANSWER_CONTENT);
                    a.putLong(serverContext.getLocalSettings().getUpdateTimestamp());
                    a.putInt(data.length);
                    a.put(serverContext.getLocalSettings().getUpdateSignature());
                    a.put(data);
                    a.flip();
                    peer.writeBufferLock.lock();
                    try {
                        if (peer.writeBuffer.remaining() < a.remaining()) {
                            ByteBuffer allocate = ByteBuffer
                                    .allocate(peer.writeBuffer.capacity() + a.remaining() + 1024 * 1024 * 10);
                            peer.writeBuffer.flip();
                            allocate.put(peer.writeBuffer);
                            peer.writeBuffer = allocate;
                        }
                        peer.writeBuffer.put(a.array());
                        peer.setWriteBufferFilled();
                    } finally {
                        peer.writeBufferLock.unlock();
                    }
                } catch (FileNotFoundException e) {
                    Log.sentry(e);
                    e.printStackTrace();
                } catch (IOException e) {
                    Log.sentry(e);
                    Log.sentry(e);
                }
            } finally {
                ConnectionReaderThread.updateUploadLock.release();
            }
        };
        ConnectionReaderThread.threadPool.submit(runnable);
        return 1;
    }

    private int handleUpdateAnswerContent(ByteBuffer readBuffer, Peer peer) {
        if (8 + 4 + MIN_SIGNATURE_LEN > readBuffer.remaining()) {
            return 0;
        }
        long othersTimestamp = readBuffer.getLong();
        int toReadBytes = readBuffer.getInt();
        byte[] signatureBytes = Utils.readSignature(readBuffer);
        if (signatureBytes == null) {
            return 0;
        }
        int lenOfSignature = signatureBytes.length;
        if (toReadBytes > readBuffer.remaining()) {
            return 0;
        }
        byte[] data = new byte[toReadBytes];
        readBuffer.get(data);
        if (othersTimestamp > serverContext.getLocalSettings().getUpdateTimestamp()) {
            try (FileOutputStream fos = new FileOutputStream("tmp_redpanda.jar")) {
                fos.write(data);
            } catch (IOException e) {
                Log.sentry(e);
            }
        }
        return 1 + 8 + 4 + lenOfSignature + data.length;
    }

    private int handleAndroidUpdateRequestTimestamp(Peer peer) {
        File file = new File(ConnectionReaderThread.ANDROID_UPDATE_FILE);
        if (!file.exists()) {
            return 1;
        }
        peer.writeBufferLock.lock();
        peer.getWriteBuffer().put(Command.ANDROID_UPDATE_ANSWER_TIMESTAMP);
        peer.getWriteBuffer().putLong(serverContext.getLocalSettings().getUpdateAndroidTimestamp());
        peer.writeBufferLock.unlock();
        peer.setWriteBufferFilled();
        return 1;
    }

    private int handleAndroidUpdateAnswerTimestamp(ByteBuffer readBuffer, Peer peer) {
        if (8 > readBuffer.remaining()) {
            return 0;
        }
        long othersTimestamp = readBuffer.getLong();
        Log.put("Update found from: " + new Date(othersTimestamp) + " our version is from: "
                + new Date(serverContext.getLocalSettings().getUpdateAndroidTimestamp()), 70);
        if (othersTimestamp < serverContext.getLocalSettings().getUpdateAndroidTimestamp()) {
            System.out.println("WARNING: peer has outdated android.apk version! " + peer.getNodeId());
        }
        if (othersTimestamp > serverContext.getLocalSettings().getUpdateAndroidTimestamp()) {
            Runnable runnable = () -> {
                ConnectionReaderThread.updateUploadLock.acquireUninterruptibly();
                try {
                    if (othersTimestamp <= serverContext.getLocalSettings().getUpdateAndroidTimestamp()) {
                        return;
                    }
                    System.out
                            .println("our android.apk version is outdated, we try to download it from this peer!");
                    peer.writeBufferLock.lock();
                    peer.writeBuffer.put(Command.ANDROID_UPDATE_REQUEST_CONTENT);
                    peer.writeBufferLock.unlock();
                    peer.setWriteBufferFilled();
                    try {
                        Thread.sleep(60000);
                    } catch (InterruptedException ignored) {
                    }
                } finally {
                    System.out.println("we can now download it from another peer...");
                    ConnectionReaderThread.updateUploadLock.release();
                }
            };
            InboundCommandProcessor.this.serverContext.getNodeStore();
            ConnectionReaderThread.threadPool.submit(runnable);
        }
        return 1 + 8;
    }

    private int handleAndroidUpdateRequestContent(Peer peer) {
        if (serverContext.getLocalSettings().getUpdateAndroidSignature() == null) {
            System.out.println(
                    "we dont have an official signature to upload that android.apk update to other peers!");
            return 1;
        }
        Runnable runnable = () -> {
            ConnectionReaderThread.updateUploadLock.acquireUninterruptibly();
            try {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {
                }
                Path path = Paths.get(ConnectionReaderThread.ANDROID_UPDATE_FILE);
                try {
                    System.out.println("we send the android.apk update to a peer!");
                    byte[] data = Files.readAllBytes(path);
                    NodeId publicUpdaterKey = Updater.getPublicUpdaterKey();
                    ByteBuffer bytesToHash = ByteBuffer.allocate(8 + data.length);
                    bytesToHash.putLong(serverContext.getLocalSettings().getUpdateAndroidTimestamp());
                    bytesToHash.put(data);
                    boolean verify = publicUpdaterKey.verify(bytesToHash.array(),
                            serverContext.getLocalSettings().getUpdateAndroidSignature());
                    if (!verify) {
                        System.out.println("################################ update not verified "
                                + serverContext.getLocalSettings().getUpdateAndroidTimestamp());
                        return;
                    }
                    byte[] androidSignature = serverContext.getLocalSettings().getUpdateAndroidSignature();
                    ByteBuffer a = ByteBuffer.allocate(1 + 8 + 4 + androidSignature.length + data.length);
                    a.put(Command.ANDROID_UPDATE_ANSWER_CONTENT);
                    a.putLong(serverContext.getLocalSettings().getUpdateAndroidTimestamp());
                    a.putInt(data.length);
                    a.put(androidSignature);
                    a.put(data);
                    a.flip();
                    peer.writeBufferLock.lock();
                    try {
                        if (peer.writeBuffer.remaining() < a.remaining()) {
                            ByteBuffer allocate = ByteBuffer
                                    .allocate(peer.writeBuffer.capacity() + a.remaining() + 1024 * 1024 * 10);
                            peer.writeBuffer.flip();
                            allocate.put(peer.writeBuffer);
                            peer.writeBuffer = allocate;
                        }
                        peer.writeBuffer.put(a.array());
                        peer.setWriteBufferFilled();
                    } finally {
                        peer.writeBufferLock.unlock();
                    }
                    int cnt = 0;
                    while (cnt < 6) {
                        cnt++;
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException ignored) {
                        }
                        peer.writeBufferLock.lock();
                        try {
                            if (!peer.isConnected() || (peer.writeBuffer.position() == 0
                                    && peer.writeBufferCrypted.position() == 0)) {
                                break;
                            }
                        } finally {
                            peer.writeBufferLock.unlock();
                        }
                        System.out.println("peer still downloading...");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } finally {
                ConnectionReaderThread.updateUploadLock.release();
            }
        };
        ConnectionReaderThread.threadPool.submit(runnable);
        return 1;
    }

    private int handleAndroidUpdateAnswerContent(ByteBuffer readBuffer, Peer peer) {
        if (8 + 4 + MIN_SIGNATURE_LEN > readBuffer.remaining()) {
            return 0;
        }
        long othersTimestamp = readBuffer.getLong();
        int toReadBytes = readBuffer.getInt();
        byte[] signature = Utils.readSignature(readBuffer);
        if (signature == null) {
            return 0;
        }
        int signatureLen = signature.length;
        if (toReadBytes > readBuffer.remaining()) {
            return 0;
        }
        byte[] data = new byte[toReadBytes];
        readBuffer.get(data);
        if (othersTimestamp > serverContext.getLocalSettings().getUpdateAndroidTimestamp()) {
            try (FileOutputStream fos = new FileOutputStream(ConnectionReaderThread.ANDROID_UPDATE_FILE)) {
                fos.write(data);
            } catch (IOException e) {
                e.printStackTrace();
            }
            serverContext.getLocalSettings().setUpdateAndroidTimestamp(othersTimestamp);
            serverContext.getLocalSettings().setUpdateAndroidSignature(signature);
        }
        return 1 + 8 + 4 + signatureLen + data.length;
    }

    private void handleJobAck(byte[] payload, Peer peer) throws InvalidProtocolBufferException {
        JobAck ackMsg = JobAck.parseFrom(payload);
        int jobId = ackMsg.getJobId();
        var runningJob = Job.getRunningJob(jobId);
        if (runningJob instanceof KademliaInsertJob) {
            ((KademliaInsertJob) runningJob).ack(peer);
            System.out.println("ACK from peer: " + peer.getNodeId().toString());
        }
    }

    private void handleKademliaGet(byte[] payload, Peer peer) throws InvalidProtocolBufferException {
        KademliaGet getMsg = KademliaGet.parseFrom(payload);
        int jobId = getMsg.getJobId();
        var searchedId = new KademliaId(getMsg.getSearchedId().getKeyBytes().toByteArray());
        var kadContent = serverContext.getKadStoreManager().get(searchedId);
        if (kadContent != null) {
            peer.getWriteBufferLock().lock();
            try {
                var answerMsg = KademliaGetAnswer.newBuilder()
                        .setAckId(jobId)
                        .setTimestamp(kadContent.getTimestamp())
                        .setPublicKey(copyFrom(kadContent.getPubkey()))
                        .setContent(copyFrom(kadContent.getContent()))
                        .setSignature(copyFrom(kadContent.getSignature()))
                        .build();
                byte[] answerData = answerMsg.toByteArray();
                peer.getWriteBuffer().put(Command.KADEMLIA_GET_ANSWER);
                peer.getWriteBuffer().putInt(answerData.length);
                peer.getWriteBuffer().put(answerData);
                peer.setWriteBufferFilled();
            } finally {
                peer.getWriteBufferLock().unlock();
            }
        } else {
            new KademliaSearchJobAnswerPeer(serverContext, searchedId, peer, jobId).start();
        }
    }

    private void handleKademliaStore(byte[] payload, Peer peer) throws InvalidProtocolBufferException {
        KademliaStore storeMsg = KademliaStore.parseFrom(payload);
        int jobId = storeMsg.getJobId();
        var kadContent = new KadContent(
                storeMsg.getTimestamp(),
                storeMsg.getPublicKey().toByteArray(),
                storeMsg.getContent().toByteArray(),
                storeMsg.getSignature().toByteArray());
        if (kadContent.verify()) {
            serverContext.getKadStoreManager().put(kadContent);
            if (jobId != 0) {
                peer.getWriteBufferLock().lock();
                try {
                    var ackMsg = JobAck.newBuilder().setJobId(jobId).build();
                    byte[] ackData = ackMsg.toByteArray();
                    peer.getWriteBuffer().put(Command.JOB_ACK);
                    peer.getWriteBuffer().putInt(ackData.length);
                    peer.getWriteBuffer().put(ackData);
                    peer.setWriteBufferFilled();
                } finally {
                    peer.getWriteBufferLock().unlock();
                }
            }
        } else {
            logger.error("Kademlia content verification failed!");
        }
    }

    private void handleKademliaGetAnswer(byte[] payload, Peer peer) throws InvalidProtocolBufferException {
        KademliaGetAnswer answerMsg = KademliaGetAnswer.parseFrom(payload);
        var kadContent = new KadContent(
                answerMsg.getTimestamp(),
                answerMsg.getPublicKey().toByteArray(),
                answerMsg.getContent().toByteArray(),
                answerMsg.getSignature().toByteArray());
        if (kadContent.verify()) {
            var byId = Job.getRunningJob(answerMsg.getAckId());
            if (byId instanceof KademliaSearchJob) {
                ((KademliaSearchJob) byId).ack(kadContent, peer);
            }
        } else {
            logger.error("Kademlia content verification failed!");
        }
    }

    private void handleFlaschenpostPut(byte[] payload, Peer peer) throws InvalidProtocolBufferException {
        FlaschenpostPut putMsg = FlaschenpostPut.parseFrom(payload);
        GMContent gmContent = GMParser.parse(serverContext, putMsg.getContent().toByteArray());
    }

    private int parseKademliaGetAnswer(ByteBuffer readBuffer, Peer peer) {
        if (4 + 8 + NodeId.PUBLIC_KEYLEN + 4 + MIN_SIGNATURE_LEN > readBuffer.remaining()) {
            return 0;
        }
        int ackId = readBuffer.getInt();
        long timestamp = readBuffer.getLong();
        byte[] publicKeyBytes = new byte[NodeId.PUBLIC_KEYLEN];
        readBuffer.get(publicKeyBytes);
        int contentLen = readBuffer.getInt();
        if (contentLen > readBuffer.remaining()) {
            return 0;
        }
        if (contentLen < 0 && contentLen > 1024 * 1024 * 10) {
            peer.disconnect("wrong contentLen for kadcontent");
            return 0;
        }
        byte[] contentBytes = new byte[contentLen];
        readBuffer.get(contentBytes);
        if (MIN_SIGNATURE_LEN > readBuffer.remaining()) {
            return 0;
        }
        byte[] signatureBytes = Utils.readSignature(readBuffer);
        if (signatureBytes == null) {
            return 0;
        }
        int lenOfSignature = signatureBytes.length;
        KadContent kadContent = new KadContent(timestamp, publicKeyBytes, contentBytes, signatureBytes);
        if (kadContent.verify()) {
            boolean saved = serverContext.getKadStoreManager().put(kadContent);
            KademliaSearchJob runningJob = (KademliaSearchJob) Job.getRunningJob(ackId);
            if (runningJob != null) {
                runningJob.ack(kadContent, peer);
            }
        } else {
            System.out.println("kadContent verification failed!!!");
        }
        return 1 + 4 + 8 + NodeId.PUBLIC_KEYLEN + 4 + contentLen + lenOfSignature;
    }

    private static String parseString(ByteBuffer buffer) {
        if (buffer.remaining() < 4) {
            return null;
        }
        int stringSize = buffer.getInt();
        if (buffer.remaining() < stringSize) {
            return null;
        }
        byte[] bytes = new byte[stringSize];
        buffer.get(bytes);
        return new String(bytes);
    }
}
