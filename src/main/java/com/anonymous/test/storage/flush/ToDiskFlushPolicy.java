package com.anonymous.test.storage.flush;

import com.anonymous.test.index.recovery.LeafNodeStatusRecorder;
import com.anonymous.test.storage.StorageLayerName;
import com.anonymous.test.storage.layer.StorageLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * @author anonymous
 * @create 2021-11-01 12:46 PM
 **/
public class ToDiskFlushPolicy extends FlushPolicy {

    private int flushCount;

    private LeafNodeStatusRecorder leafNodeStatusRecorder = null;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public ToDiskFlushPolicy() {
        super(StorageLayerName.EBS);
    }

    public String printStatus() {
        String status = "[Flush to Disk] # of flush: " + flushCount;
        return status;
    }

    @Override
    public void flush(StorageLayer storageLayerNeededFlush, StorageLayer flushToWhichStorageLayer) {
        flushCount++;

        // 1. put to the corresponding layer
        logger.info("from [{}] start flush to [{}]...", storageLayerNeededFlush.getStorageLayerName(), flushToWhichStorageLayer.getStorageLayerName());

        Set<String> inS3Blocks = new HashSet<>();
        for (String blockId : storageLayerNeededFlush.getLocalLocationMappingTable().keySet()) {
            if (!storageLayerNeededFlush.getLocalLocationMappingTable().get(blockId).isInS3()) {
                flushToWhichStorageLayer.put(storageLayerNeededFlush.get(blockId));

                // for recovery
                if (leafNodeStatusRecorder != null && "disk".equals(leafNodeStatusRecorder.getMode())) {
                    leafNodeStatusRecorder.markBlockId(blockId);
                }
            } else {
                inS3Blocks.add(blockId);
                logger.info("[{}] is already in S3", blockId);
            }
        }
        storageLayerNeededFlush.setLastFlushTimestamp(System.currentTimeMillis());

        storageLayerNeededFlush.getLocalLocationMappingTable().keySet().removeIf(inS3Blocks::contains);
        //logger.info("finishing: {}", storageLayerNeededFlush.getLocalLocationMappingTable().keySet());
        logger.info("finish flush for [{}] blocks", storageLayerNeededFlush.getLocalLocationMappingTable().keySet().size());
        // 2. remove outdated mapping
        storageLayerNeededFlush.clearAll();

        // for recovery
        if (leafNodeStatusRecorder != null && "disk".equals(leafNodeStatusRecorder.getMode())) {
            leafNodeStatusRecorder.checkAndFlushLeafNode();
        }

    }

    public void setLeafNodeStatusRecorder(LeafNodeStatusRecorder leafNodeStatusRecorder) {
        this.leafNodeStatusRecorder = leafNodeStatusRecorder;
    }
}
