package im.redpanda.core;

import im.redpanda.crypt.Sha256Hash;
import im.redpanda.crypt.Utils;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;
import org.jetbrains.annotations.NotNull;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import java.io.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.ArrayList;
import java.util.Random;

public class SmallChiperStreamTest {

    //        public static final String ALGORITHM = "AES/GCM/NoPadding";
    public static final String ALGORITHM = "AES/CTR/NoPadding";

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }


    public static void main(final String[] args) throws NoSuchProviderException, InvalidAlgorithmParameterException, NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException, IOException {

        final SmallChiperStreamTest cip = new SmallChiperStreamTest();
//        cip.runExperiments();

//        cip.runExperimentsTwo();

        Sha256Hash sha256Hash = Sha256Hash.create(Utils.parseAsHexOrBase58("0437fb5ab1b9c42505c6fff4fd9a01e8aecf52fd51e3562c5769246587d36a179f95c2748f432c508f10a3a8edf6eb12d2c3367c147892e176c5c4e0bfd2b38c9a"));

        byte[] bytes = sha256Hash.getBytes();

        System.out.println("public key sha256: " + Utils.bytesToHexString(bytes));

    }

    private void runExperiments() {

        CipherOutputStream output = null;
        CipherInputStream input = null;
//        FileOutputStream fileOutput = null;
//        FileInputStream fileInput = null;

        KeyPair keyPairA = generateECKeys();
        KeyPair keyPairB = generateECKeys();


        System.out.println("" + keyPairA.getPublic().getFormat());


        // Create two AES secret keys to encrypt/decrypt the message
        SecretKey secretKeyA = generateSharedSecret(keyPairA.getPrivate(),
                keyPairB.getPublic());
        SecretKey secretKeyB = generateSharedSecret(keyPairB.getPrivate(),
                keyPairA.getPublic());

        byte[] iv = new SecureRandom().generateSeed(16);
        IvParameterSpec ivSpec = new IvParameterSpec(iv);

        try {

//            fileOutput = new FileOutputStream("CipherOutput.txt");
//            fileInput = new FileInputStream("CipherOutput.txt");


//            PipedInputStream pipedInputStream = new PipedInputStream();
//            PipedOutputStream pipedOutputStream = new PipedOutputStream(pipedInputStream);

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

//            byte[] bytes = new byte[1024 * 1024];
//            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);


//            final KeyGenerator kg = KeyGenerator.getInstance("AES");
//            kg.init(new SecureRandom(new byte[]{1, 2, 3}));
//            final SecretKey key = kg.generateKey();

            final Cipher c = Cipher.getInstance(ALGORITHM, "BC");
            c.init(Cipher.ENCRYPT_MODE, secretKeyA, ivSpec);
            output = new CipherOutputStream(byteArrayOutputStream, c);
//            output = new CipherOutputStream(pipedOutputStream, c);


            final PrintWriter pw = new PrintWriter(output);
//            pw.println("Ci");
            pw.println("Cipher Streams are working correctly.1");
            pw.println("Cipher Streams are working correctly.2");
            pw.println("Cipher Streams are working correctly.3");
            pw.println("Cipher Streams are working correctly.4");
            pw.println("Cipher Streams are working correctly.5");
            pw.println("Cipher Streams are working correctly.6");
            pw.flush();
//            pw.close();


            System.out.println("" + byteArrayOutputStream.size());

            byte[] bytes1 = byteArrayOutputStream.toByteArray();
            byteArrayOutputStream.reset();


//            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes1);
//
//            System.out.println("available: " + byteArrayInputStream.available());

            MyInputStream myInputStream = new MyInputStream();

            System.out.println("" + bytes1.length);

            ByteBuffer wrap = ByteBuffer.wrap(bytes1);
            myInputStream.appendBuffer(wrap);


//            final KeyGenerator kg2 = KeyGenerator.getInstance("AES");
//            kg2.init(new SecureRandom(new byte[]{1, 2, 3}));
//            final SecretKey key2 = kg2.generateKey();

            final Cipher c2 = Cipher.getInstance(ALGORITHM, "BC");
            c2.init(Cipher.DECRYPT_MODE, secretKeyB, ivSpec);
            input = new CipherInputStream(myInputStream, c2);
//            input = new CipherInputStream(pipedInputStream, c2);


            System.out.println("inp: " + input.available());

            final InputStreamReader r = new InputStreamReader(input);
            final BufferedReader reader = new BufferedReader(r);

            System.out.println("" + input.available());

//            int read = input.read();
//            while (read != -1) {
//                read = input.read();
//                System.out.println("read: " + read);
//                pw.println("Cipher Streams are working correctly.X");
//                pw.flush();
//                bytes1 = byteArrayOutputStream.toByteArray();
//                System.out.println("" + bytes1.length);
//                wrap = ByteBuffer.wrap(bytes1);
//                myInputStream.appendBuffer(wrap);
//            }


            String line = reader.readLine();
            while (line != null) {
                System.out.println("Line: " + line);
                line = reader.readLine();
                pw.println("Cipher Streams are working correctly.X");
                pw.flush();
                bytes1 = byteArrayOutputStream.toByteArray();
                byteArrayOutputStream.reset();
                System.out.println("" + bytes1.length);
                wrap = ByteBuffer.wrap(bytes1);
                myInputStream.appendBuffer(wrap);
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

//            int cnt = 0;
//            while (cnt < 20) {
//                cnt++;
//                System.out.println("inp: " + input.available());
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }

        } catch (NoSuchAlgorithmException e) {
            System.out.println("Specified Algorithm does not exist");
        } catch (NoSuchPaddingException e) {
            System.out.println("Specified Padding does not exist");
        } catch (FileNotFoundException e) {
            System.out.println("Could not find specified file to read / write to");
        } catch (InvalidKeyException e) {
            System.out.println("Specified key is invalid");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException from BufferedReader when reading file");
            e.printStackTrace();
        } catch (InvalidAlgorithmParameterException e) {
            e.printStackTrace();
        } catch (NoSuchProviderException e) {
            e.printStackTrace();
        } finally {
            try {
                if (output != null) {
                    output.flush();
                    output.close();
                }
                if (input != null) {
                    input.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public void runExperimentsTwo() throws NoSuchPaddingException, NoSuchAlgorithmException, NoSuchProviderException, InvalidAlgorithmParameterException, InvalidKeyException, IOException {

        CipherOutputStreamByteBuffer output = null;
        CipherInputStream input = null;
//        FileOutputStream fileOutput = null;
//        FileInputStream fileInput = null;

        KeyPair keyPairA = generateECKeys();
        KeyPair keyPairB = generateECKeys();


        System.out.println("" + keyPairA.getPublic().getFormat());


        // Create two AES secret keys to encrypt/decrypt the message
        SecretKey secretKeyA = generateSharedSecret(keyPairA.getPrivate(),
                keyPairB.getPublic());
        SecretKey secretKeyB = generateSharedSecret(keyPairB.getPrivate(),
                keyPairA.getPublic());

        byte[] iv = new SecureRandom().generateSeed(16);
        IvParameterSpec ivSpec = new IvParameterSpec(iv);


//            fileOutput = new FileOutputStream("CipherOutput.txt");
//            fileInput = new FileInputStream("CipherOutput.txt");


//            PipedInputStream pipedInputStream = new PipedInputStream();
//            PipedOutputStream pipedOutputStream = new PipedOutputStream(pipedInputStream);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

//            byte[] bytes = new byte[1024 * 1024];
//            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);


//            final KeyGenerator kg = KeyGenerator.getInstance("AES");
//            kg.init(new SecureRandom(new byte[]{1, 2, 3}));
//            final SecretKey key = kg.generateKey();

        final Cipher c = Cipher.getInstance(ALGORITHM, "SunJCE");
        System.out.println(c.getProvider().getName());
        c.init(Cipher.ENCRYPT_MODE, secretKeyA, ivSpec);
//        output = new CipherOutputStream(byteArrayOutputStream, c);
        output = new CipherOutputStreamByteBuffer(byteArrayOutputStream, c);


        output.write(new byte[1]);
        output.flush();

        System.out.println("" + byteArrayOutputStream.size());

        final PrintWriter pw = new PrintWriter(output);
        pw.print("C");
        pw.flush();
//            pw.close();

        System.out.println("" + byteArrayOutputStream.size());


        while (byteArrayOutputStream.size() == 0) {
            System.out.println("" + byteArrayOutputStream.size());
            pw.println("i");
            pw.flush();
            output.flush();
        }
        System.out.println("" + byteArrayOutputStream.size());

        byte[] bytes1 = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.reset();

    }


    public static class MyOutputStream extends OutputStream {

        @Override
        public void write(int b) throws IOException {

        }
    }

    public static class MyInputStream extends InputStream {

        ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

        public void appendBuffer(ByteBuffer buffer) {
            buffers.add(buffer);
        }

        @Override
        public int available() throws IOException {
            System.out.println("available...");
            if (buffers.isEmpty()) {
                return 0;
            }

            return buffers.get(0).remaining();
        }

        @Override
        public int read() throws IOException {
            if (buffers.isEmpty()) {
                return -1;
            }

            ByteBuffer byteBuffer = buffers.get(0);

            if (!byteBuffer.hasRemaining()) {
                return -1;
            }

//            System.out.println("" + byteBuffer.remaining());

            if (Math.random() < 0.5) {
                System.out.println("-1 ############");
                return -1;
            }

            return byteBuffer.get();
        }

        @Override
        public int read(@NotNull byte[] b) throws IOException {
//            System.out.println("read 2");
            if (buffers.isEmpty()) {
                return -1;
            }

            ByteBuffer byteBuffer = buffers.get(0);

            if (!byteBuffer.hasRemaining()) {
                buffers.remove(0);

                if (buffers.size() == 0) {
                    return -1;
                }

                byteBuffer = buffers.get(0);
                if (!byteBuffer.hasRemaining()) {
                    return -1;
                }
            }

            int i = new Random().nextInt(20);

//            if (i < 1) {
//                return (byte) 44;
//            }

            int min = Math.min(byteBuffer.remaining(), b.length);
            min = Math.min(min, i);


            byteBuffer.get(b, 0, min);


            return min;
        }

        @Override
        public int read(@NotNull byte[] b, int off, int len) throws IOException {
            throw new RuntimeException("not implemented by now, maybe this is not needed.... code: 4hdfg5tj5n");
//            if (buffers.isEmpty()) {
//                return -1;
//            }
//
//            ByteBuffer byteBuffer = buffers.get(0);
//
//            if (!byteBuffer.hasRemaining()) {
//                return -1;
//            }
//
//            byteBuffer.get(b, off, len);
//
//            System.out.println("read 3");
//            return super.read(b, off, len);
        }
    }

    public static KeyPair generateECKeys() {
        try {
            ECNamedCurveParameterSpec parameterSpec = ECNamedCurveTable.getParameterSpec("brainpoolp256r1");
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(
                    "ECDH", "BC");

            keyPairGenerator.initialize(parameterSpec);
            KeyPair keyPair = keyPairGenerator.generateKeyPair();

            return keyPair;
        } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException
                | NoSuchProviderException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static SecretKey generateSharedSecret(PrivateKey privateKey,
                                                 PublicKey publicKey) {
        try {
            KeyAgreement keyAgreement = KeyAgreement.getInstance("ECDH", "BC");
            keyAgreement.init(privateKey);
            keyAgreement.doPhase(publicKey, true);

            SecretKey key = keyAgreement.generateSecret("AES");
            return key;
        } catch (InvalidKeyException | NoSuchAlgorithmException
                | NoSuchProviderException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

}