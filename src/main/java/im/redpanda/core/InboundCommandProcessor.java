package im.redpanda.core;

import im.redpanda.core.exceptions.PeerProtocolException;
import im.redpanda.crypt.Utils;
import im.redpanda.flaschenpost.GMContent;
import im.redpanda.flaschenpost.GMParser;
import im.redpanda.jobs.Job;
import im.redpanda.jobs.KademliaInsertJob;
import im.redpanda.jobs.KademliaSearchJob;
import im.redpanda.jobs.KademliaSearchJobAnswerPeer;
import im.redpanda.kademlia.KadContent;
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
            Log.put("Received ping command", 200);
            if (!serverContext.getPeerList().contains(peer.getKademliaId())) {
                logger.error(String.format("Got PING from node not in our peerlist, lets add it.... %s, id: %s", peer, peer.getKademliaId()));
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
        if (command == Command.PONG) {
            Log.put("Received pong command", 200);
            peer.ping = (1 * peer.ping + (double) (System.currentTimeMillis() - peer.lastPinged)) / 2;
            peer.setLastPongReceived(System.currentTimeMillis());
            return 1;
        } else if (command == Command.REQUEST_PEERLIST) {
            serverContext.getPeerList().getReadWriteLock().readLock().lock();
            ByteBuffer byteBuffer = ByteBufferPool.borrowObject(1024 * 1024);
            try {
                try {
                    int size = 0;
                    ArrayList<Peer> peerToWrite = new ArrayList<>();
                    for (Peer peerToCheck : serverContext.getPeerList().getPeerArrayList()) {
                        size += 4; // len of ip string
                        if (peerToCheck.getNodeId() != null && peerToCheck.getNodeId().hasKey()) {
                            size += 2 + NodeId.PUBLIC_KEYLEN; // bool + pubkey
                        } else {
                            size += 2; // bool false
                        }
                        size += 4; // port
                        peerToWrite.add(peerToCheck);
                    }
                    int maxLen = 1024 * 1024 - 64; // keep slack
                    int cnt = 0;
                    byteBuffer.putInt(0); // placeholder for count
                    for (Peer peerToWriteObj : peerToWrite) {
                        if (byteBuffer.position() > maxLen) break;
                        if (peerToWriteObj.getNodeId() != null && peerToWriteObj.getNodeId().hasKey()) {
                            byteBuffer.putShort((short) 1);
                            byteBuffer.put(peerToWriteObj.getNodeId().exportPublic());
                        } else {
                            byteBuffer.putShort((short) 0);
                        }
                        byte[] ipStringBytes = peerToWriteObj.ip.getBytes();
                        byteBuffer.putInt(ipStringBytes.length);
                        byteBuffer.put(ipStringBytes);
                        byteBuffer.putInt(peerToWriteObj.getPort());
                        cnt++;
                    }
                    byteBuffer.putInt(0, cnt);
                    byteBuffer.flip();
                    peer.getWriteBufferLock().lock();
                    try {
                        peer.writeBuffer.put(Command.SEND_PEERLIST);
                        peer.writeBuffer.putInt(byteBuffer.remaining());
                        peer.writeBuffer.put(byteBuffer);
                        peer.setWriteBufferFilled();
                        byteBuffer.compact();
                    } finally {
                        peer.getWriteBufferLock().unlock();
                    }
                } finally {
                    ByteBufferPool.returnObject(byteBuffer);
                }
            } finally {
                serverContext.getPeerList().getReadWriteLock().readLock().unlock();
            }
            return 1;
        } else if (command == Command.SEND_PEERLIST) {
            int toRead = readBuffer.getInt();
            if (readBuffer.remaining() < toRead) {
                return 0;
            }
            byte[] bytesForPeerList = new byte[toRead];
            readBuffer.get(bytesForPeerList);
            ByteBuffer peerListBytes = ByteBuffer.wrap(bytesForPeerList);
            int peerListSize = peerListBytes.getInt();
            for (int i = 0; i < peerListSize; i++) {
                NodeId nodeId = null;
                int booleanNodeIdPresent = peerListBytes.getShort();
                if (booleanNodeIdPresent == 1) {
                    byte[] bytes = new byte[NodeId.PUBLIC_KEYLEN];
                    peerListBytes.get(bytes);
                    nodeId = NodeId.importPublic(bytes);
                }
                String ip = parseString(peerListBytes);
                int port = peerListBytes.getInt();
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
                    Node byKademliaId = Node.getByKademliaId(serverContext, nodeId.getKademliaId());
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
            return 1 + 4 + toRead;
        } else if (command == Command.UPDATE_REQUEST_TIMESTAMP) {
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
        } else if (command == Command.UPDATE_ANSWER_TIMESTAMP) {
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
                        try { Thread.sleep(60000); } catch (InterruptedException ignored) {}
                    } finally {
                        System.out.println("we can now download it from another peer...");
                        ConnectionReaderThread.updateDownloadLock.unlock();
                    }
                };
                Server.threadPool.submit(runnable);
            }
            return 1 + 8;
        } else if (command == Command.UPDATE_REQUEST_CONTENT) {
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
                    try { Thread.sleep(200); } catch (InterruptedException ignored) {}
                    Path path = Settings.isSeedNode() ? Paths.get("target/redpanda.jar") : Paths.get("redpanda.jar");
                    try {
                        System.out.println("we send the update to a peer!");
                        byte[] data = Files.readAllBytes(path);
                        ByteBuffer a = ByteBuffer.allocate(1 + 8 + 4 + serverContext.getLocalSettings().getUpdateSignature().length + data.length);
                        a.put(Command.UPDATE_ANSWER_CONTENT);
                        a.putLong(serverContext.getLocalSettings().getUpdateTimestamp());
                        a.putInt(data.length);
                        a.put(serverContext.getLocalSettings().getUpdateSignature());
                        a.put(data);
                        a.flip();
                        peer.writeBufferLock.lock();
                        try {
                            if (peer.writeBuffer.remaining() < a.remaining()) {
                                ByteBuffer allocate = ByteBuffer.allocate(peer.writeBuffer.capacity() + a.remaining() + 1024 * 1024 * 10);
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
        } else if (command == Command.UPDATE_ANSWER_CONTENT) {
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
        } else if (command == Command.ANDROID_UPDATE_REQUEST_TIMESTAMP) {
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
        } else if (command == Command.ANDROID_UPDATE_ANSWER_TIMESTAMP) {
            if (8 > readBuffer.remaining()) {
                return 0;
            }
            long othersTimestamp = readBuffer.getLong();
            Log.put("Update found from: " + new Date(othersTimestamp) + " our version is from: " + new Date(serverContext.getLocalSettings().getUpdateAndroidTimestamp()), 70);
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
                        System.out.println("our android.apk version is outdated, we try to download it from this peer!");
                        peer.writeBufferLock.lock();
                        peer.writeBuffer.put(Command.ANDROID_UPDATE_REQUEST_CONTENT);
                        peer.writeBufferLock.unlock();
                        peer.setWriteBufferFilled();
                        try { Thread.sleep(60000); } catch (InterruptedException ignored) {}
                    } finally {
                        System.out.println("we can now download it from another peer...");
                        ConnectionReaderThread.updateUploadLock.release();
                    }
                };
                InboundCommandProcessor.this.serverContext.getNodeStore();
                ConnectionReaderThread.threadPool.submit(runnable);
            }
            return 1 + 8;
        } else if (command == Command.ANDROID_UPDATE_REQUEST_CONTENT) {
            if (serverContext.getLocalSettings().getUpdateAndroidSignature() == null) {
                System.out.println("we dont have an official signature to upload that android.apk update to other peers!");
                return 1;
            }
            Runnable runnable = () -> {
                ConnectionReaderThread.updateUploadLock.acquireUninterruptibly();
                try {
                    try { Thread.sleep(200); } catch (InterruptedException ignored) {}
                    Path path = Paths.get(ConnectionReaderThread.ANDROID_UPDATE_FILE);
                    try {
                        System.out.println("we send the android.apk update to a peer!");
                        byte[] data = Files.readAllBytes(path);
                        NodeId publicUpdaterKey = Updater.getPublicUpdaterKey();
                        ByteBuffer bytesToHash = ByteBuffer.allocate(8 + data.length);
                        bytesToHash.putLong(serverContext.getLocalSettings().getUpdateAndroidTimestamp());
                        bytesToHash.put(data);
                        boolean verify = publicUpdaterKey.verify(bytesToHash.array(), serverContext.getLocalSettings().getUpdateAndroidSignature());
                        if (!verify) {
                            System.out.println("################################ update not verified " + serverContext.getLocalSettings().getUpdateAndroidTimestamp());
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
                                ByteBuffer allocate = ByteBuffer.allocate(peer.writeBuffer.capacity() + a.remaining() + 1024 * 1024 * 10);
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
                            try { Thread.sleep(10000); } catch (InterruptedException ignored) {}
                            peer.writeBufferLock.lock();
                            try {
                                if (!peer.isConnected() || (peer.writeBuffer.position() == 0 && peer.writeBufferCrypted.position() == 0)) {
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
        } else if (command == Command.ANDROID_UPDATE_ANSWER_CONTENT) {
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
        } else if (command == Command.JOB_ACK) {
            int jobId = readBuffer.getInt();
            Job runningJob = Job.getRunningJob(jobId);
            if (runningJob instanceof KademliaInsertJob) {
                KademliaInsertJob job = (KademliaInsertJob) runningJob;
                job.ack(peer);
                System.out.println("ACK from peer: " + peer.getNodeId().toString());
            }
            return 1 + 4;
        } else if (command == Command.KADEMLIA_GET) {
            if (readBuffer.remaining() < 4 + KademliaId.ID_LENGTH_BYTES) {
                return 0;
            }
            int jobId = readBuffer.getInt();
            byte[] kadIdBytes = new byte[KademliaId.ID_LENGTH_BYTES];
            readBuffer.get(kadIdBytes);
            KademliaId searchedId = new KademliaId(kadIdBytes);
            KadContent kadContent = serverContext.getKadStoreManager().get(searchedId);
            if (kadContent != null) {
                peer.getWriteBufferLock().lock();
                try {
                    peer.getWriteBuffer().put(Command.KADEMLIA_GET_ANSWER);
                    peer.getWriteBuffer().putInt(jobId);
                    peer.getWriteBuffer().putLong(kadContent.getTimestamp());
                    peer.getWriteBuffer().put(kadContent.getPubkey());
                    peer.getWriteBuffer().putInt(kadContent.getContent().length);
                    peer.getWriteBuffer().put(kadContent.getContent());
                    peer.getWriteBuffer().put(kadContent.getSignature());
                } finally {
                    peer.getWriteBufferLock().unlock();
                }
            } else {
                new KademliaSearchJobAnswerPeer(serverContext, searchedId, peer, jobId).start();
            }
            return 1 + 4 + KademliaId.ID_LENGTH_BYTES;
        } else if (command == Command.KADEMLIA_GET_ANSWER) {
            return parseKademliaGetAnswer(readBuffer, peer);
        } else if (command == Command.FLASCHENPOST_PUT) {
            int contentLen = readBuffer.getInt();
            if (readBuffer.remaining() < contentLen) {
                return 0;
            }
            byte[] content = new byte[contentLen];
            readBuffer.get(content);
            GMContent gmContent = GMParser.parse(serverContext, content);
            return 1 + 4 + contentLen;
        }
        throw new RuntimeException("Got unknown command from peer: " + command + " last cmd: " + peer.lastCommand + " lightClient: " + peer.isLightClient());
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
