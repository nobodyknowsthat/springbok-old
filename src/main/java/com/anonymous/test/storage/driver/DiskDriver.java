package com.anonymous.test.storage.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Date 2021/4/25 11:20
 * @Created by anonymous
 */
public class DiskDriver implements PersistenceDriver {

    private String rootUri;  // path uri, used to construct uri for specific object

    public DiskDriver(String rootUri) {
        this.rootUri = rootUri;
        File file = new File(rootUri);
        if (!file.exists()) {
            file.mkdir();
        }
    }

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * users should construct the full path before call this function
     * @param key
     * @param dataString
     */
    @Override
    public void flush(String key, String dataString) {
        File file = new File(key);
        try {
            if (!file.exists()) {
                file.createNewFile();
                logger.info(key + " file been created.");
            }

            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            fileOutputStream.write(dataString.getBytes());
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush(String key, String dataString, Map<String, String> metadataMap) {

    }

    @Override
    public Map<String, String> getMetaDataAsString(String key) {
        return null;
    }

    @Override
    public List<String> listKeysWithSamePrefix(String prefix) {
        return null;
    }

    @Override
    public void flush(String key, byte[] dataBytes) {
        File file = new File(key);
        try {
            if (!file.exists()) {
                file.createNewFile();
                logger.info(key + " file been created.");
            }

            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            fileOutputStream.write(dataBytes);
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getDataAsString(String key) {
        File file = new File(key);

        if (!file.exists()) {
            logger.info("file not exist");
            return "";
        }
        long length = file.length();
        if (length > Integer.MAX_VALUE) {
            throw new RuntimeException("Not support such large file now");
        }
        int lengthInt = (int) length;
        return getDataAsStringPartial(key, 0, lengthInt);
    }

    @Override
    public byte[] getDataAsByteArray(String key) {
        File file = new File(key);

        if (!file.exists()) {
            logger.info("file not exist");
            return new byte[0];
        }
        long length = file.length();
        if (length > Integer.MAX_VALUE) {
            throw new RuntimeException("Not support such large file now");
        }
        int lengthInt = (int) length;
        return getDataAsByteArrayPartial(key, 0, lengthInt);
    }

    /**
     * key is file path
     * @param key
     * @param offset  the start point of reading in source file
     * @param length
     * @return
     */
    @Override
    public String getDataAsStringPartial(String key, int offset, int length) {
        File file = new File(key);
        try {
            if (!file.exists()) {
                logger.info("file not exist");
                return "";
            }

            /*FileInputStream fileInputStream = new FileInputStream(file);
            byte[] buffer = new byte[length];
            fileInputStream.getChannel().position();
            fileInputStream.read();*/

            byte[] buffer = new byte[length];
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(offset);
            randomAccessFile.read(buffer, 0, length);
            randomAccessFile.close();
            return new String(buffer);

        } catch (IOException e) {
            e.printStackTrace();
        }


        return "";
    }

    @Override
    public byte[] getDataAsByteArrayPartial(String key, int offset, int length) {
        File file = new File(key);
        try {
            if (!file.exists()) {
                logger.info("file not exist");
                return new byte[0];
            }


            byte[] buffer = new byte[length];
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(offset);
            randomAccessFile.read(buffer, 0, length);
            randomAccessFile.close();
            return buffer;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return new byte[0];
    }

    public int getFileSize(String filename) {
        File file = new File(filename);
        if (file.exists()) {
            long fileSize = file.length();
            if (fileSize > Integer.MAX_VALUE) {
                throw new RuntimeException("Not support such large file now");
            }

            return (int) fileSize;
        } else {
            logger.info("file [{}] not exist", filename);
            return 0;
        }
    }

    @Override
    public String getRootUri() {
        return this.rootUri;
    }

    @Override
    public void remove(String key) {
        File file = new File(key);

        if (!file.exists()) {
            logger.info("file not exist");
        }
        file.delete();
    }

    @Override
    public void close() {
        // do nothing for disk driver
    }

    @Override
    public long getObjectSize(String key) {
        // not used for disk driver
        return 0;
    }
}
