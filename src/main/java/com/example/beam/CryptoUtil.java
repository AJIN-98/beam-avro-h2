package com.example.beam;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

public final class CryptoUtil {
  private static final String TRANSFORMATION = "AES/GCM/NoPadding";
  private static final int IV_BYTES = 12;      // Recommended for GCM
  private static final int TAG_BITS = 128;     // 16 bytes tag

  private CryptoUtil() {}

  public static String generateKeyB64(int bits) {
    try {
      KeyGenerator kg = KeyGenerator.getInstance("AES");
      kg.init(bits);
      SecretKey key = kg.generateKey();
      return Base64.getEncoder().encodeToString(key.getEncoded());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String encryptToB64(String plaintext, String base64Key) {
    try {
      byte[] keyBytes = Base64.getDecoder().decode(base64Key);
      SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");

      byte[] iv = new byte[IV_BYTES];
      new SecureRandom().nextBytes(iv);

      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
      byte[] ct = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

      ByteBuffer bb = ByteBuffer.allocate(iv.length + ct.length);
      bb.put(iv).put(ct);
      return Base64.getEncoder().encodeToString(bb.array());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String decryptFromB64(String b64Ciphertext, String base64Key) {
    try {
      byte[] payload = Base64.getDecoder().decode(b64Ciphertext);
      byte[] iv = new byte[IV_BYTES];
      byte[] ct = new byte[payload.length - IV_BYTES];
      System.arraycopy(payload, 0, iv, 0, IV_BYTES);
      System.arraycopy(payload, IV_BYTES, ct, 0, ct.length);

      byte[] keyBytes = Base64.getDecoder().decode(base64Key);
      SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");

      Cipher cipher = Cipher.getInstance(TRANSFORMATION);
      cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
      byte[] pt = cipher.doFinal(ct);
      return new String(pt, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
