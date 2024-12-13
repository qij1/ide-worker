package com.boncfc.ide.plugin.task.api.utils;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

public class AESUtil {
    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/CBC/PKCS5Padding";
    private static final int KEY_LENGTH = 16;

    /**
     * AES加密方法
     *
     * @param plainText 明文
     * @param key      密钥
     * @return 加密后的密文（Base64编码）
     */
    public static String encrypt(String plainText, byte[] key) {
        try {
            key = Arrays.copyOf(key, KEY_LENGTH);
            SecretKeySpec secretKeySpec = new SecretKeySpec(key, ALGORITHM);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            IvParameterSpec ivParameterSpec = new IvParameterSpec(new byte[KEY_LENGTH]);
            cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivParameterSpec);

            byte[] encryptedBytes = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(encryptedBytes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * AES解密方法
     *
     * @param cipherText 密文（Base64编码）
     * @param key       密钥
     * @return 解密后的明文
     */
    public static String decrypt(String cipherText, byte[] key) {
        try {
            key = Arrays.copyOf(key, KEY_LENGTH);
            SecretKeySpec secretKeySpec = new SecretKeySpec(key, ALGORITHM);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            IvParameterSpec ivParameterSpec = new IvParameterSpec(new byte[KEY_LENGTH]);
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, ivParameterSpec);

            byte[] encryptedBytes = Base64.getDecoder().decode(cipherText);
            byte[] decryptedBytes = cipher.doFinal(encryptedBytes);
            return new String(decryptedBytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        String plainText = "ide";
        byte[] key = "mySaltValue".getBytes(StandardCharsets.UTF_8);

        // 加密
        String encryptedText = encrypt(plainText, key);
        System.out.println("加密后的密文: " + encryptedText);

        // 解密
        String decryptedText = decrypt(encryptedText, key);
        System.out.println("解密后的明文: " + decryptedText);
    }
}
