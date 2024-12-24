import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static com.boncfc.ide.plugin.task.api.utils.AESUtil.decrypt;
import static com.boncfc.ide.plugin.task.api.utils.AESUtil.encrypt;

public class AESTest {

    @Test
    public void testAES() {
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
