package surfstore;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class BlockUtil {

    private static final int BLOCKSIZE = 4 * 1024;

    static String sha256(String s) {
        MessageDigest digest;

        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            throw new RuntimeException("No such hash algorithm");
        }

        byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
        String encoded = Base64.getEncoder().encodeToString(hash);

        return encoded;
    }

    static List<String> readBlocksFromFile(String filepath) {
        List<String> blocks = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(filepath)) {
            byte[] buf = new byte[BLOCKSIZE];
            int size;

            while ((size = fis.read(buf)) != -1) {
                blocks.add(new String(buf, 0, size, StandardCharsets.UTF_8));
                buf = new byte[BLOCKSIZE];
            }
        } catch (FileNotFoundException e) {
            return null;
        } catch (IOException e) {
            e.printStackTrace();
           return null;
        }

        return blocks;
    }

    static Map<String, byte[]> scanBlocksInDir(String dir) {
        File dirFile = new File(dir);
        File[] files = dirFile.listFiles(File::isFile);

        if (files == null) {
            return null;
        }

        Map<String, byte[]> hashBlockMap = new HashMap<>();
        for (File file : files) {
            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] buf = new byte[BLOCKSIZE];
                int size;

                while ((size = fis.read(buf)) != -1) {
                    String block = new String(buf, 0, size, StandardCharsets.UTF_8);
                    buf = size < BLOCKSIZE ? Arrays.copyOfRange(buf, 0, size) : buf;
                    hashBlockMap.put(sha256(block), buf);
                    buf = new byte[BLOCKSIZE];
                }
            } catch (FileNotFoundException e) {
                return null;
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        return hashBlockMap;
    }

    static boolean writeBlocksToFile(String filename, String dir, List<byte[]> blocks) {
        File writeFile = Paths.get(dir, filename).toFile();

        try (FileOutputStream fos = new FileOutputStream(writeFile)) {
            for (byte[] block : blocks) {
                fos.write(block);
            }
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public static void main(String[] args) {

    }
}
