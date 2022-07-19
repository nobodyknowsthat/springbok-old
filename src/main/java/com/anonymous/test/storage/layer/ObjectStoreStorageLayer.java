package com.anonymous.test.storage.layer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.anonymous.test.storage.*;
import com.anonymous.test.storage.driver.ObjectStoreDriver;
import com.anonymous.test.storage.flush.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import java.util.*;

/**
 * we store blocks in the object store such as AWS S3
 * @author anonymous
 * @create 2021-09-20 9:15 PM
 **/
public class ObjectStoreStorageLayer extends StorageLayer {

    private ObjectStoreDriver objectStoreDriver;

    private String bucketName;

    private Region region;

    private ObjectMapper objectMapper;

    private S3LayoutSchema s3LayoutSchema;

    private Map<String, List<String>> simplePrefixKeyListCache = new HashMap<>();

    private Map<String, String> simpleMetaObjectCache = new HashMap<>(); // key is metadata object key, value is metadata

    private Map<String, String> simpleDataObjectCache = new HashMap<>(); // key is data object key

    private Queue<String> dataObjectCacheQueue = new LinkedList<>();  // used to kick out data object

    private int dataCacheSize = 100;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private int putCount = 0;

    public ObjectStoreStorageLayer(FlushPolicy flushPolicy, String bucketName, Region region, S3LayoutSchema layoutSchema) {
        super(flushPolicy);
        this.bucketName = bucketName;
        this.region = region;
        this.objectStoreDriver = new ObjectStoreDriver(bucketName, region);
        this.objectMapper = new ObjectMapper();
        this.s3LayoutSchema = layoutSchema;
        this.setStorageLayerName(StorageLayerName.S3);

    }

    public String printStatus() {
        String status = "[Object Storage Layer] S3 layout scheme = " + s3LayoutSchema.toString() + ", flushBlockNumThreshold = " + getFlushBlockNumThreshold() + ", flushTimeThreshold" + getFlushTimeThreshold() +
                "\n # of put requests (# of objects): " + putCount;
        return status;
    }

    @Override
    public boolean isFlushNeeded() {
        return false;
    }

    /**
     * invoke by flush()
     * @param block
     */
    @Override
    public void put(Block block) {

        // 1. put to AWS S3
        String blockId = block.getBlockId();
        if (block.getMetaDataString() == null) {
            putCount++;
            objectStoreDriver.flush(blockId, block.getDataString());
            logger.info("[{}] has been put to S3", blockId);
        } else {

            objectStoreDriver.flush(blockId, block.getDataString());
            objectStoreDriver.flush(blockId+".mapping", block.getMetaDataString());
            putCount = putCount + 2;

            logger.info("[{}] has been put to S3", blockId);
            logger.info("[{}] has been put to S3", blockId+".mapping");
        }


    }

//    static int dataCacheCount = 0;
//    static int dataS3PartialCount = 0;
//    static int dataS3FullCount = 0;
//    static Set<String> uniqueObjectKeySet = new HashSet<>();

    @Override
    public Block get(String blockId) {

        Block block = new Block();
        String blockString;
        BlockLocation blockLocation = getBlockLocation(blockId);
        //uniqueObjectKeySet.add(blockLocation.getFilepath());
        //System.out.println(blockLocation);
        if (!blockLocation.isRange()) {

            if (simpleDataObjectCache.containsKey(blockLocation.getFilepath())) {
                blockString = simpleDataObjectCache.get(blockLocation.getFilepath());
                logger.info("Get block [{}] from simple data cache", blockLocation.getFilepath());
            } else {
                blockString = objectStoreDriver.getDataAsString(blockLocation.getFilepath());
                logger.info("Get block [{}] from S3", blockLocation.getFilepath());
                if (simpleDataObjectCache.keySet().size() > dataCacheSize) {
                    String key = dataObjectCacheQueue.poll();
                    simpleDataObjectCache.remove(key);
                }
                simpleDataObjectCache.put(blockLocation.getFilepath(), blockString);
                dataObjectCacheQueue.offer(blockLocation.getFilepath());
            }
        } else {
            //System.out.println(blockLocation);
            if (simpleDataObjectCache.containsKey(blockLocation.getFilepath())) {
                blockString = simpleDataObjectCache.get(blockLocation.getFilepath());
                blockString = blockString.substring(blockLocation.getOffset(), blockLocation.getOffset() + blockLocation.getLength());
                logger.info("Get block [{}] from simple data cache", blockLocation.getFilepath());
                //System.out.println("data cache");
                //dataCacheCount++;
            } else {
                long objectSize = getObjectSize(blockLocation.getFilepath());
                //System.out.println("object size: " + objectSize);
                if (objectSize > 1024 * 1024 * 64) {

                    blockString = objectStoreDriver.getDataAsStringPartial(blockLocation.getFilepath(), blockLocation.getOffset(), blockLocation.getLength());
                    logger.info("Get block [{}] from S3 in a partial way", blockLocation.getFilepath());
                    //dataS3PartialCount++;
                } else {
                    blockString = objectStoreDriver.getDataAsString(blockLocation.getFilepath());
                    logger.info("Get block [{}] from S3 in a full way", blockLocation.getFilepath());
                    if (simpleDataObjectCache.keySet().size() > dataCacheSize) {
                        String key = dataObjectCacheQueue.poll();
                        simpleDataObjectCache.remove(key);
                    }
                    simpleDataObjectCache.put(blockLocation.getFilepath(), blockString);
                    dataObjectCacheQueue.offer(blockLocation.getFilepath());
                    blockString = blockString.substring(blockLocation.getOffset(), blockLocation.getOffset() + blockLocation.getLength());
                    //dataS3FullCount++;

                }
            }
        }
        block.setBlockId(blockId);
        block.setDataString(blockString);

//        System.out.println("data cache count: " + dataCacheCount);
//        System.out.println("s3 partial count: " + dataS3PartialCount);
//        System.out.println("s3 full count: " + dataS3FullCount);
//        System.out.println("unique object size: " + uniqueObjectKeySet.size());

        return block;
    }

    private Map<String, Long> objectSizeMap = new HashMap<>();
    private long getObjectSize(String key) {
        if (objectSizeMap.containsKey(key)) {
            return objectSizeMap.get(key);
        } else {
            long size = objectStoreDriver.getObjectSize(key);
            objectSizeMap.put(key, size);
            return size;
        }
    }

    @Override
    public BlockLocation getBlockLocation(String blockId) {

        if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.DIRECT)) {
            return new BlockLocation(blockId, false);
        } else if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.SINGLE_TRAJECTORY)) {
            return getBlockLocationForSingleTrajectoryLayout(blockId);
        } else if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.SPATIO_TEMPORAL)) {
            return getBlockLocationForSpatioTemporalLayout(blockId);
        } else if (s3LayoutSchema.getS3LayoutSchemaName().equals(S3LayoutSchemaName.SPATIO_TEMPORAL_STR)) {
            return getBlockLocationForSpatioTemporalSTRLayout(blockId);
        } else {
            throw new UnsupportedOperationException("please specify s3 layout schema");
        }

    }

    private BlockLocation getBlockLocationForSingleTrajectoryLayout(String blockId) {
        String newBlockIdPrefix = S3SingleTrajectoryLayoutSchemaTool.generateMetaDataObjectKeyForQuery(blockId, s3LayoutSchema.getTimePartitionLength());
        // get metadata of this object
        List<String> keyList;
        if (simplePrefixKeyListCache.containsKey(newBlockIdPrefix)) {
            keyList = simplePrefixKeyListCache.get(newBlockIdPrefix);
        } else {
            keyList = objectStoreDriver.listKeysWithSamePrefix(newBlockIdPrefix);
            simplePrefixKeyListCache.put(newBlockIdPrefix, keyList);
        }
        List<String> metadataKeyList = new ArrayList<>();
        for (String key : keyList) {
            if (key.endsWith(".mapping")) {
                metadataKeyList.add(key);
            }
        }

        BlockLocation blockLocation = null;
        for (String metadataKey : metadataKeyList) {
            //String metadata = objectStoreDriver.getDataAsString(metadataKey);
            String metadata = "";
            if (simpleMetaObjectCache.containsKey(metadataKey)) {
                metadata = simpleMetaObjectCache.get(metadataKey);
                logger.info("get this metadata [{}] from simple meta object cache", metadataKey);
            } else {
                metadata = objectStoreDriver.getDataAsString(metadataKey);
                simpleMetaObjectCache.put(metadataKey, metadata);
            }

            Map<String, String> blockLocationMap = null;
            try {
                blockLocationMap = objectMapper.readValue(metadata, Map.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            if (blockLocationMap.containsKey(blockId)) {
                String blockLocationString = blockLocationMap.get(blockId);
                try {
                    blockLocation = objectMapper.readValue(blockLocationString, BlockLocation.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                break;
            }
        }
        blockLocation.setStorageLayerName(StorageLayerName.S3);
        blockLocation.setRange(true);
        return blockLocation;
    }

    private BlockLocation getBlockLocationForSpatioTemporalSTRLayout(String blockId) {
        String newBlockIdPrefix = S3SpatioTemporalSTRLayoutSchemaTool.generateMetaDataObjectKeyForQuery(blockId, s3LayoutSchema.getTimePartitionLength());
        // get metadata of this object
        List<String> keyList;
        if (simplePrefixKeyListCache.containsKey(newBlockIdPrefix)) {
            keyList = simplePrefixKeyListCache.get(newBlockIdPrefix);
        } else {
            keyList = objectStoreDriver.listKeysWithSamePrefix(newBlockIdPrefix);
            simplePrefixKeyListCache.put(newBlockIdPrefix, keyList);
        }

        List<String> metadataKeyList = new ArrayList<>();
        for (String key : keyList) {
            if (key.endsWith(".mapping")) {
                metadataKeyList.add(key);
            }
        }

        BlockLocation blockLocation = null;
        for (String metadataKey : metadataKeyList) {
            //String metadata = objectStoreDriver.getDataAsString(metadataKey);
            String metadata = "";
            if (simpleMetaObjectCache.containsKey(metadataKey)) {
                metadata = simpleMetaObjectCache.get(metadataKey);
                logger.info("get this metadata [{}] from simple meta object cache", metadataKey);
            } else {
                metadata = objectStoreDriver.getDataAsString(metadataKey);
                simpleMetaObjectCache.put(metadataKey, metadata);
            }

            Map<String, String> blockLocationMap = null;
            try {
                blockLocationMap = objectMapper.readValue(metadata, Map.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            if (blockLocationMap.containsKey(blockId)) {
                String blockLocationString = blockLocationMap.get(blockId);
                try {
                    blockLocation = objectMapper.readValue(blockLocationString, BlockLocation.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                break;
            }
        }
        blockLocation.setStorageLayerName(StorageLayerName.S3);
        blockLocation.setRange(true);
        return blockLocation;
    }

    private BlockLocation getBlockLocationForSpatioTemporalLayout(String blockId) {

        logger.info(blockId);
        String newBlockIdPrefix = S3SpatioTemporalLayoutSchemaTool.generateMetaDataObjectKeyForQuery(blockId, s3LayoutSchema.getTimePartitionLength(), s3LayoutSchema.getSpatialRightShiftBitNum());
        // get metadata of this object
        List<String> keyList;
        if (simplePrefixKeyListCache.containsKey(newBlockIdPrefix)) {
            keyList = simplePrefixKeyListCache.get(newBlockIdPrefix);
        } else {
            keyList = objectStoreDriver.listKeysWithSamePrefix(newBlockIdPrefix);
            simplePrefixKeyListCache.put(newBlockIdPrefix, keyList);
        }

        List<String> metadataKeyList = new ArrayList<>();
        for (String key : keyList) {
            if (key.endsWith(".mapping")) {
                metadataKeyList.add(key);
            }
        }
        logger.info("metadata object number: " + metadataKeyList.size() + ", value: " + metadataKeyList);
        BlockLocation blockLocation = null;
        for (String metadataKey : metadataKeyList) {
            String metadata = "";
            if (simpleMetaObjectCache.containsKey(metadataKey)) {
                metadata = simpleMetaObjectCache.get(metadataKey);
                logger.info("get this metadata [{}] from simple meta object cache", metadataKey);
            } else {
                metadata = objectStoreDriver.getDataAsString(metadataKey);
                simpleMetaObjectCache.put(metadataKey, metadata);
            }

            Map<String, String> blockLocationMap = null;
            try {
                blockLocationMap = objectMapper.readValue(metadata, Map.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            if (blockLocationMap.containsKey(blockId)) {
                String blockLocationString = blockLocationMap.get(blockId);
                try {
                    blockLocation = objectMapper.readValue(blockLocationString, BlockLocation.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                break;
            }
        }
        blockLocation.setStorageLayerName(StorageLayerName.S3);
        blockLocation.setRange(true);

        return blockLocation;
    }

    @Override
    public void flush() {
        // do nothing
    }

    @Override
    public void batchPut(List<Block> blockList) {
        for (Block block : blockList) {
            put(block);
        }
    }

    @Override
    public List<Block> batchGet(List<String> blockIdList) {
        List<Block> resultBlockList = new ArrayList<>();

        /*String blockString;
        List<BlockLocation> blockLocationList = new ArrayList<>();
        for (String blockId : blockIdList) {
            BlockLocation blockLocation = getBlockLocation(blockId);
            if (!blockLocation.isRange()) {
                // if an object contains only one block, directly get it
                if (simpleDataObjectCache.containsKey(blockLocation.getFilepath())) {
                    blockString = simpleDataObjectCache.get(blockLocation.getFilepath());
                    logger.info("Get block [{}] from simple data cache", blockLocation.getFilepath());
                } else {
                    blockString = objectStoreDriver.getDataAsString(blockLocation.getFilepath());
                    logger.info("Get block [{}] from S3", blockLocation.getFilepath());
                    if (simpleDataObjectCache.keySet().size() > dataCacheSize) {
                        String key = dataObjectCacheQueue.poll();
                        simpleDataObjectCache.remove(key);
                    }
                    simpleDataObjectCache.put(blockLocation.getFilepath(), blockString);
                    dataObjectCacheQueue.offer(blockLocation.getFilepath());
                }
                Block block = new Block();
                block.setBlockId(blockId);
                block.setDataString(blockString);
                resultBlockList.add(block);
            } else {
                if (simpleDataObjectCache.containsKey(blockLocation.getFilepath())) {
                    blockString = simpleDataObjectCache.get(blockLocation.getFilepath());
                    blockString = blockString.substring(blockLocation.getOffset(), blockLocation.getOffset() + blockLocation.getLength());
                    logger.info("Get block [{}] from simple data cache", blockLocation.getFilepath());
                    Block block = new Block();
                    block.setBlockId(blockId);
                    block.setDataString(blockString);
                    resultBlockList.add(block);
                } else {
                    blockLocationList.add(blockLocation);
                }
            }
        }

        // group the chunks in the same object
        Map<String, List<BlockLocation>> groupMap = new HashMap<>();
        for (BlockLocation blockLocation : blockLocationList) {
            String objectKey = blockLocation.getFilepath();
            if (groupMap.containsKey(objectKey)) {
                groupMap.get(objectKey).add(blockLocation);
            } else {
                List<BlockLocation> blockLocationsInObject = new ArrayList<>();
                blockLocationsInObject.add(blockLocation);
                groupMap.put(objectKey, blockLocationsInObject);
            }
        }

        for (String objectKey : groupMap.keySet()) {

        }*/

        for (String blockId : blockIdList) {
            resultBlockList.add(get(blockId));
        }

        return resultBlockList;
    }

    @Override
    public void clearAll() {
        // remove data
        objectStoreDriver.deleteBucket(bucketName);
        logger.info("clear blocks in S3");

    }

    @Override
    public void clear(List<String> blockIdList) {
        // remove data
        for (String blockId : blockIdList) {
            objectStoreDriver.remove(blockId);
        }
        logger.info("clear blocks in S3: [{}]", blockIdList);

    }

    @Override
    public void close() {
        objectStoreDriver.close();
        simpleMetaObjectCache.clear();
    }
}
