package com.anonymous.test.storage.flush;

import com.anonymous.test.storage.StorageLayerName;
import com.anonymous.test.storage.layer.StorageLayer;

/**
 * @author anonymous
 * @create 2021-09-20 11:00 AM
 **/
public abstract class FlushPolicy {

    private StorageLayerName flushToWhichStorageLayerName;

    public FlushPolicy(StorageLayerName flushToWhichStorageLayerName) {
        this.flushToWhichStorageLayerName = flushToWhichStorageLayerName;
    }

    public abstract void flush(StorageLayer storageLayerNeededFlush, StorageLayer flushToWhichStorageLayer);

    public StorageLayerName getFlushToWhichStorageLayerName() {
        return flushToWhichStorageLayerName;
    }

}
