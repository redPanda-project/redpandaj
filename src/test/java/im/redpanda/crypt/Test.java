package im.redpanda.crypt;

import java.security.spec.AlgorithmParameterSpec;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class Test {
    // static String plainText = "This is a plain text which will be encrypted by
    // ChaCha20 Poly1305 Algorithm";

    public static void main(String[] args) throws Exception {

        byte[] b = new byte[] { (byte) 1, (byte) 2, (byte) 3 };

        // Security.addProvider(new BouncyCastleProvider());

        KeyGenerator keyGenerator = KeyGenerator.getInstance("ChaCha20");
        keyGenerator.init(256);

        // Generate Key
        SecretKey key = keyGenerator.generateKey();

        System.out.println("Original Text  : " + b.length);

        byte[] cipherText = encrypt(b, key);
        System.out.println("Encrypted Text : " + cipherText.length);

        // cipherText[4] = 2;

        byte[] decrypt = decrypt(cipherText, key);
        System.out.println("DeCrypted Text : " + decrypt.length);

    }

    public static byte[] encrypt(byte[] plaintext, SecretKey key) throws Exception {
        byte[] nonceBytes = new byte[12];

        // Get Cipher Instance
        Cipher cipher = Cipher.getInstance("ChaCha20-Poly1305/None/NoPadding");

        // Create IvParamterSpec
        AlgorithmParameterSpec ivParameterSpec = new IvParameterSpec(nonceBytes);

        // Create SecretKeySpec
        SecretKeySpec keySpec = new SecretKeySpec(key.getEncoded(), "ChaCha20");

        // Initialize Cipher for ENCRYPT_MODE
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivParameterSpec);

        // Perform Encryption
        byte[] cipherText = cipher.doFinal(plaintext);

        return cipherText;
    }

    public static byte[] decrypt(byte[] cipherText, SecretKey key) throws Exception {
        byte[] nonceBytes = new byte[12];

        // Get Cipher Instance
        Cipher cipher = Cipher.getInstance("ChaCha20-Poly1305/None/NoPadding");

        // Create IvParamterSpec
        AlgorithmParameterSpec ivParameterSpec = new IvParameterSpec(nonceBytes);

        // Create SecretKeySpec
        SecretKeySpec keySpec = new SecretKeySpec(key.getEncoded(), "ChaCha20");

        // Initialize Cipher for DECRYPT_MODE
        cipher.init(Cipher.DECRYPT_MODE, keySpec, ivParameterSpec);

        // Perform Decryption
        byte[] decryptedText = cipher.doFinal(cipherText);

        return decryptedText;
    }
}