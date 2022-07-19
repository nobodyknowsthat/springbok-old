package com.anonymous.test.storage.flush;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.anonymous.test.index.recovery.LeafNodeStatusRecorder;
import com.anonymous.test.storage.Block;
import com.anonymous.test.storage.StorageLayerName;
import com.anonymous.test.storage.flush.task.S3FlushTaskForSpatioTemporal;
import com.anonymous.test.storage.layer.StorageLayer;
import com.anonymous.test.storage.BlockIdentifierEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author anonymous
 * @create 2021-11-01 2:50 PM
 **/
public class ToS3FlushPolicy extends FlushPolicy{

    private S3LayoutSchema layoutSchema;

    private int objectSize = 8;  // the number of chunks in an object

    private int numOfConnectionQueues = 1;

    private final static int S3_METADATA_SIZE_LIMIT = 1024 * 2 -4;

    private ObjectMapper objectMapper = new ObjectMapper();

    private int flushCount = 0;

    private LeafNodeStatusRecorder leafNodeStatusRecorder = null;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public ToS3FlushPolicy(S3LayoutSchema layoutSchema) {
        super(StorageLayerName.S3);
        this.layoutSchema = layoutSchema;
    }

    public ToS3FlushPolicy(S3LayoutSchema layoutSchema, int objectSize, int numOfConnectionQueues) {
        super(StorageLayerName.S3);
        this.layoutSchema = layoutSchema;
        this.objectSize = objectSize;
        this.numOfConnectionQueues = numOfConnectionQueues;
    }

    public String printStatus() {
        String status = "[Flush to S3] # of flush: " + flushCount;
        return status;
    }

    @Override
    public void flush(StorageLayer storageLayerNeededFlush, StorageLayer flushToWhichStorageLayer) {
        flushCount++;
        if (layoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.DIRECT)) {
            flushWithDirectLayout(storageLayerNeededFlush, flushToWhichStorageLayer);
        } else if (layoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.SINGLE_TRAJECTORY)) {
            flushWithSingleTrajectoryLayout(storageLayerNeededFlush, flushToWhichStorageLayer);
        } else if (layoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.SPATIO_TEMPORAL)) {
            flushWithSpatioTemporalLayoutParallel(storageLayerNeededFlush, flushToWhichStorageLayer);
        } else if (layoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.SPATIO_TEMPORAL_STR)) {
            flushWithSpatioTemporalSTRLayout(storageLayerNeededFlush, flushToWhichStorageLayer);
        } else {
            throw new UnsupportedOperationException("please specify an S3 layout schema");
        }
        storageLayerNeededFlush.setLastFlushTimestamp(System.currentTimeMillis());
        storageLayerNeededFlush.clearAll();
    }

    protected void flushWithSpatioTemporalLayoutParallel(StorageLayer storageLayerNeededFlush, StorageLayer flushToWhichStorageLayer) {
        List<String> blockIdList = new ArrayList<>();
        for (String blockId : storageLayerNeededFlush.getLocalLocationMappingTable().keySet()) {
            if (!storageLayerNeededFlush.getLocalLocationMappingTable().get(blockId).isInS3()) {
                blockIdList.add(blockId);
            }
        }

        // separate block ids according to time partition in the s3 layout schema

        int timePartitionLength = layoutSchema.getTimePartitionLength();
        Map<String, List<String>> timePartitionedBlockIdsMap = new HashMap<>();
        for (String blockId : blockIdList) {
            BlockIdentifierEntity entity = BlockIdentifierEntity.decoupleBlockIdForSpatioTemporalLayout(blockId);
            long timestamp = entity.getTimestamp();
            String key = String.valueOf(S3SpatioTemporalLayoutSchemaTool.generateTimePartitionId(timestamp, timePartitionLength));
            if (timePartitionedBlockIdsMap.containsKey(key)) {
                timePartitionedBlockIdsMap.get(key).add(blockId);
            } else {
                List<String> partitionIds = new ArrayList<>();
                partitionIds.add(blockId);
                timePartitionedBlockIdsMap.put(key, partitionIds);
            }
        }

        for (String key : sortKeys(timePartitionedBlockIdsMap.keySet())) {
            List<Queue<String>> queueList = S3SpatioTemporalLayoutSchemaTool.generateQueueList(timePartitionedBlockIdsMap.get(key), numOfConnectionQueues, layoutSchema.getSpatialRightShiftBitNum());

            ExecutorService executorService = Executors.newFixedThreadPool(numOfConnectionQueues);
            CountDownLatch countDownLatch = new CountDownLatch(numOfConnectionQueues);

            for (Queue<String> queue : queueList) {
                executorService.execute(new S3FlushTaskForSpatioTemporal(countDownLatch, queue, storageLayerNeededFlush, flushToWhichStorageLayer, this, objectSize));
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            executorService.shutdown();
        }

    }

    @Deprecated
    protected void flushWithSpatioTemporalLayout(StorageLayer storageLayerNeededFlush, StorageLayer flushToWhichStorageLayer) {
        // test with single thread
        List<String> blockIdList = new ArrayList<>();
        for (String blockId : storageLayerNeededFlush.getLocalLocationMappingTable().keySet()) {
            if (!storageLayerNeededFlush.getLocalLocationMappingTable().get(blockId).isInS3()) {
                blockIdList.add(blockId);
            }
        }
        List<Queue<String>> queueList = S3SpatioTemporalLayoutSchemaTool.generateQueueList(blockIdList, numOfConnectionQueues, layoutSchema.getSpatialRightShiftBitNum());

        for (Queue<String> queue : queueList) {
            //System.out.println(queue);
            List<String> blockIdsInObject = new ArrayList<>();
            long lastSpatialPrefix = -1;
            while(!queue.isEmpty()) {
                String blockId = queue.poll();
                BlockIdentifierEntity blockIdentifierEntity = BlockIdentifierEntity.decoupleBlockIdForSpatioTemporalLayout(blockId);
                blockIdentifierEntity.setSpatialPartitionId(S3SpatioTemporalLayoutSchemaTool.generateSpatialPartitionId(blockIdentifierEntity.getSpatialPointEncoding(), layoutSchema.getSpatialRightShiftBitNum()));

                // check if it is a new  spatial partition
                if (blockIdentifierEntity.getSpatialPartitionId() != lastSpatialPrefix && lastSpatialPrefix != -1) {

                    if (blockIdsInObject.size() > 0) {
                        List<Block> blockListInObject = new ArrayList<>();
                        for (String blockIdInObject : blockIdsInObject) {
                            blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
                        }
                        Block assembledBlock = S3SpatioTemporalLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockListInObject, layoutSchema);
                        //System.out.println(assembledBlock);
                        flushToWhichStorageLayer.put(assembledBlock);
                        // reset
                        blockIdsInObject.clear();
                    }

                    blockIdsInObject.add(blockId);
                    lastSpatialPrefix = blockIdentifierEntity.getSpatialPartitionId();

                    if (blockIdsInObject.size() >= objectSize) {
                        // only go here where each object contains one chunk
                        // assemble and flush
                        List<Block> blockListInObject = new ArrayList<>();
                        for (String blockIdInObject : blockIdsInObject) {
                            blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
                        }
                        Block assembledBlock = S3SpatioTemporalLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockListInObject, layoutSchema);
                        //System.out.println(assembledBlock);
                        flushToWhichStorageLayer.put(assembledBlock);
                        // reset
                        blockIdsInObject.clear();
                    }
                    continue;
                }

                // if not a new partition, check whether it is full
                if (blockIdsInObject.size() >= (objectSize - 1)) {
                    blockIdsInObject.add(blockId);
                    lastSpatialPrefix = blockIdentifierEntity.getSpatialPartitionId();
                    // assemble and flush
                    List<Block> blockListInObject = new ArrayList<>();
                    for (String blockIdInObject : blockIdsInObject) {
                        blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
                    }
                    Block assembledBlock = S3SpatioTemporalLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockListInObject, layoutSchema);
                    //System.out.println(assembledBlock);
                    flushToWhichStorageLayer.put(assembledBlock);
                    // reset
                    blockIdsInObject.clear();
                } else {
                    blockIdsInObject.add(blockId);
                    lastSpatialPrefix = blockIdentifierEntity.getSpatialPartitionId();
                }

            }

            // the remaining ids must come from the same spatial partition
            if (blockIdsInObject.size() > 0) {
                List<Block> blockListInObject = new ArrayList<>();
                for (String blockIdInObject : blockIdsInObject) {
                    blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
                }
                Block assembledBlock = S3SpatioTemporalLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockListInObject, layoutSchema);
                //System.out.println(assembledBlock);
                flushToWhichStorageLayer.put(assembledBlock);
                // reset
                blockIdsInObject.clear();
            }
        }
    }


    protected void flushWithSingleTrajectoryLayout(StorageLayer storageLayerNeededFlush, StorageLayer flushToWhichStorageLayer) {
        List<String> blockIdList = new ArrayList<>();
        for (String blockId : storageLayerNeededFlush.getLocalLocationMappingTable().keySet()) {
            if (!storageLayerNeededFlush.getLocalLocationMappingTable().get(blockId).isInS3()) {
                blockIdList.add(blockId);
            }
        }

        // separate block ids according to time partition in the s3 layout schema
        int timePartitionLength = layoutSchema.getTimePartitionLength();
        Map<String, List<String>> timePartitionedBlockIdsMap = new HashMap<>();
        for (String blockId : blockIdList) {
            BlockIdentifierEntity entity = BlockIdentifierEntity.decoupleBlockIdForSingleTrajectoryLayout(blockId);
            long timestamp = entity.getTimestamp();
            String key = String.valueOf(S3SingleTrajectoryLayoutSchemaTool.generateTimePartitionId(timestamp, timePartitionLength));
            if (timePartitionedBlockIdsMap.containsKey(key)) {
                timePartitionedBlockIdsMap.get(key).add(blockId);
            } else {
                List<String> partitionIds = new ArrayList<>();
                partitionIds.add(blockId);
                timePartitionedBlockIdsMap.put(key, partitionIds);
            }
        }

        for (String key : sortKeys(timePartitionedBlockIdsMap.keySet())) {

            List<Queue<String>> queueList = S3SingleTrajectoryLayoutSchemaTool.generateQueueList(timePartitionedBlockIdsMap.get(key), numOfConnectionQueues);
            for (Queue<String> queue : queueList) {
                String lastSid = null;
                List<String> blockIdsInObject = new ArrayList<>();
                while (!queue.isEmpty()) {
                    String blockId = queue.poll();
                    BlockIdentifierEntity blockIdentifierEntity = BlockIdentifierEntity.decoupleBlockIdForSingleTrajectoryLayout(blockId);
                    // check if it is a new  spatial partition
                    if (!blockIdentifierEntity.getSid().equals(lastSid) && lastSid != null) {

                        if (blockIdsInObject.size() > 0) {
                            List<Block> blockListInObject = new ArrayList<>();
                            for (String blockIdInObject : blockIdsInObject) {
                                blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
                            }
                            Block assembledBlock = S3SingleTrajectoryLayoutSchemaTool.assembleBlocksForSingleTrajectoryLayout(blockListInObject, layoutSchema);
                            //System.out.println(assembledBlock);
                            flushToWhichStorageLayer.put(assembledBlock);

                            // for recovery
                            if (leafNodeStatusRecorder != null && "object".equals(leafNodeStatusRecorder.getMode())) {
                                leafNodeStatusRecorder.markBlockIds(blockIdsInObject);
                            }

                            // reset
                            blockIdsInObject.clear();
                        }

                        blockIdsInObject.add(blockId);
                        lastSid = blockIdentifierEntity.getSid();

                        if (blockIdsInObject.size() >= objectSize) {
                            // only go here where each object contains one chunk
                            // assemble and flush
                            List<Block> blockListInObject = new ArrayList<>();
                            for (String blockIdInObject : blockIdsInObject) {
                                blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
                            }
                            Block assembledBlock = S3SingleTrajectoryLayoutSchemaTool.assembleBlocksForSingleTrajectoryLayout(blockListInObject, layoutSchema);
                            //System.out.println(assembledBlock);
                            flushToWhichStorageLayer.put(assembledBlock);

                            // for recovery
                            if (leafNodeStatusRecorder != null && "object".equals(leafNodeStatusRecorder.getMode())) {
                                leafNodeStatusRecorder.markBlockIds(blockIdsInObject);
                            }

                            // reset
                            blockIdsInObject.clear();
                        }
                        continue;
                    }

                    // if not a new partition, check whether it is full
                    if (blockIdsInObject.size() >= (objectSize - 1)) {
                        blockIdsInObject.add(blockId);
                        lastSid = blockIdentifierEntity.getSid();
                        // assemble and flush
                        List<Block> blockListInObject = new ArrayList<>();
                        for (String blockIdInObject : blockIdsInObject) {
                            blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
                        }
                        Block assembledBlock = S3SingleTrajectoryLayoutSchemaTool.assembleBlocksForSingleTrajectoryLayout(blockListInObject, layoutSchema);
                        //System.out.println(assembledBlock);
                        flushToWhichStorageLayer.put(assembledBlock);

                        // for recovery
                        if (leafNodeStatusRecorder != null && "object".equals(leafNodeStatusRecorder.getMode())) {
                            leafNodeStatusRecorder.markBlockIds(blockIdsInObject);
                        }

                        // reset
                        blockIdsInObject.clear();
                    } else {
                        blockIdsInObject.add(blockId);
                        lastSid = blockIdentifierEntity.getSid();
                    }

                }

                // the remaining ids must come from the same spatial partition
                if (blockIdsInObject.size() > 0) {
                    List<Block> blockListInObject = new ArrayList<>();
                    for (String blockIdInObject : blockIdsInObject) {
                        blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
                    }
                    Block assembledBlock = S3SingleTrajectoryLayoutSchemaTool.assembleBlocksForSingleTrajectoryLayout(blockListInObject, layoutSchema);
                    //System.out.println(assembledBlock);
                    flushToWhichStorageLayer.put(assembledBlock);

                    // for recovery
                    if (leafNodeStatusRecorder != null && "object".equals(leafNodeStatusRecorder.getMode())) {
                        leafNodeStatusRecorder.markBlockIds(blockIdsInObject);
                    }

                    // reset
                    blockIdsInObject.clear();
                }


            }

        }

        // for recovery
        if (leafNodeStatusRecorder != null && "object".equals(leafNodeStatusRecorder.getMode())) {
            leafNodeStatusRecorder.checkAndFlushLeafNode();
        }

    }

    protected void flushWithSpatioTemporalSTRLayout(StorageLayer storageLayerNeededFlush, StorageLayer flushToWhichStorageLayer) {
        List<String> blockIdList = new ArrayList<>();
        for (String blockId : storageLayerNeededFlush.getLocalLocationMappingTable().keySet()) {
            if (!storageLayerNeededFlush.getLocalLocationMappingTable().get(blockId).isInS3()) {
                blockIdList.add(blockId);
            }
        }

        // separate block ids according to time partition in the s3 layout schema
        int timePartitionLength = layoutSchema.getTimePartitionLength();
        Map<String, List<String>> timePartitionedBlockIdsMap = new HashMap<>();
        for (String blockId : blockIdList) {
            BlockIdentifierEntity entity = BlockIdentifierEntity.decoupleBlockIdForSpatioTemporalLayout(blockId);
            long timestamp = entity.getTimestamp();
            String key = String.valueOf(S3SpatioTemporalSTRLayoutSchemaTool.generateTimePartitionId(timestamp, timePartitionLength));
            if (timePartitionedBlockIdsMap.containsKey(key)) {
                timePartitionedBlockIdsMap.get(key).add(blockId);
            } else {
                List<String> partitionIds = new ArrayList<>();
                partitionIds.add(blockId);
                timePartitionedBlockIdsMap.put(key, partitionIds);
            }
        }

        for (String key : sortKeys(timePartitionedBlockIdsMap.keySet())) {

            Map<String, String> metadataMap = new HashMap<>();
            List<Queue<String>> queueList = S3SpatioTemporalSTRLayoutSchemaTool.generateQueueList(timePartitionedBlockIdsMap.get(key), numOfConnectionQueues, objectSize);
            for (Queue<String> queue : queueList) {

                List<String> blockIdsInObject = new ArrayList<>();
                while (!queue.isEmpty()) {
                    String blockId = queue.poll();

                    // check whether it is full
                    if (blockIdsInObject.size() >= (objectSize - 1)) {
                        blockIdsInObject.add(blockId);

                        // assemble and flush
                        List<Block> blockListInObject = new ArrayList<>();
                        for (String blockIdInObject : blockIdsInObject) {
                            blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
                        }
                        Block assembledBlock = S3SpatioTemporalSTRLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockListInObject, layoutSchema);
                        try {
                            metadataMap.putAll(objectMapper.readValue(assembledBlock.getMetaDataString(), Map.class));
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        //System.out.println(assembledBlock);
                        assembledBlock.setMetaDataString(null);
                        flushToWhichStorageLayer.put(assembledBlock);

                        // for recovery
                        if (leafNodeStatusRecorder != null && "object".equals(leafNodeStatusRecorder.getMode())) {
                            leafNodeStatusRecorder.markBlockIds(blockIdsInObject);
                        }

                        // reset
                        blockIdsInObject.clear();
                    } else {
                        blockIdsInObject.add(blockId);
                    }

                }

                // the remaining ids must come from the same spatial partition
                if (blockIdsInObject.size() > 0) {
                    List<Block> blockListInObject = new ArrayList<>();
                    for (String blockIdInObject : blockIdsInObject) {
                        blockListInObject.add(storageLayerNeededFlush.get(blockIdInObject));
                    }
                    Block assembledBlock = S3SpatioTemporalSTRLayoutSchemaTool.assembleBlocksForSpatioTemporalLayout(blockListInObject, layoutSchema);
                    try {
                        metadataMap.putAll(objectMapper.readValue(assembledBlock.getMetaDataString(), Map.class));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    //System.out.println(assembledBlock);
                    assembledBlock.setMetaDataString(null);
                    flushToWhichStorageLayer.put(assembledBlock);
                    // for recovery
                    if (leafNodeStatusRecorder != null && "object".equals(leafNodeStatusRecorder.getMode())) {
                        leafNodeStatusRecorder.markBlockIds(blockIdsInObject);
                    }

                    // reset
                    blockIdsInObject.clear();
                }


            }

            String blockId = S3SpatioTemporalSTRLayoutSchemaTool.generateMetaDataObjectKeyForPut(timePartitionedBlockIdsMap.get(key).get(0), layoutSchema.getTimePartitionLength()) + ".mapping";
            //System.out.println("===========" + blockId);
            Block metaDataBlock = null;
            try {
                metaDataBlock = new Block(blockId, objectMapper.writeValueAsString(metadataMap));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            flushToWhichStorageLayer.put(metaDataBlock);
        }

        // for recovery
        if (leafNodeStatusRecorder != null && "object".equals(leafNodeStatusRecorder.getMode())) {
            leafNodeStatusRecorder.checkAndFlushLeafNode();
        }
    }

    protected void flushWithDirectLayout(StorageLayer storageLayerNeededFlush, StorageLayer flushToWhichStorageLayer) {
        // 1. put to the corresponding layer
        logger.info("from [{}] start flush to [{}]...", storageLayerNeededFlush.getStorageLayerName(), flushToWhichStorageLayer.getStorageLayerName());

        Set<String> inS3Blocks = new HashSet<>();
        for (String blockId : storageLayerNeededFlush.getLocalLocationMappingTable().keySet()) {
            if (!storageLayerNeededFlush.getLocalLocationMappingTable().get(blockId).isInS3()) {
                flushToWhichStorageLayer.put(storageLayerNeededFlush.get(blockId));
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
    }

    protected void flushWithTemporalLayout(StorageLayer storageLayerNeededFlush, StorageLayer flushToWhichStorageLayer) {
        // not used
    }


    private static List<String> sortKeys(Set<String> keys) {
        List<Long> keyLongList = new ArrayList<>();
        for (String key : keys) {
            keyLongList.add(Long.valueOf(key));
        }

        keyLongList.sort(Comparator.comparingLong(Long::longValue));

        List<String> keyList = new ArrayList<>();
        for (Long longValue : keyLongList) {
            keyList.add(String.valueOf(longValue));
        }

        return keyList;
    }

    public S3LayoutSchema getLayoutSchema() {
        return layoutSchema;
    }

    public LeafNodeStatusRecorder getLeafNodeStatusRecorder() {
        return leafNodeStatusRecorder;
    }

    public void setLeafNodeStatusRecorder(LeafNodeStatusRecorder leafNodeStatusRecorder) {
        this.leafNodeStatusRecorder = leafNodeStatusRecorder;
    }
}
