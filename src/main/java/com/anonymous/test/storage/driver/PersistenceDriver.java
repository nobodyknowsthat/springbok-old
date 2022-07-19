package com.anonymous.test.storage.driver;

import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Date 2021/4/25 11:14
 * @Created by anonymous
 */
public interface PersistenceDriver {

    // only for test; key is file path for disk file and object key name for S3
    void flush(String key, String dataString);

    void flush(String key, String dataString, Map<String, String> metadataMap);

    void flush(String key, byte[] dataBytes);

    String getDataAsString(String key);

    Map<String, String> getMetaDataAsString(String key);

    List<String> listKeysWithSamePrefix(String prefix);

    byte[] getDataAsByteArray(String key);

    String getDataAsStringPartial(String key, int offset, int length);

    byte[] getDataAsByteArrayPartial(String key, int offset, int length);

    long getObjectSize(String key);

    void remove(String key);

    String getRootUri();

    void close();
}
