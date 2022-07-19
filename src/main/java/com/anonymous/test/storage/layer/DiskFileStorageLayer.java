package com.anonymous.test.storage.layer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.anonymous.test.storage.Block;
import com.anonymous.test.storage.BlockLocation;
import com.anonymous.test.storage.StorageLayerName;
import com.anonymous.test.storage.driver.DiskDriver;
import com.anonymous.test.storage.flush.FlushPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * in this disk layer, we store all block in files, each file have a fixed size, if file size exceeds a threshold, create a new one
 * @author anonymous
 * @create 2021-09-20 9:02 PM
 **/
public class DiskFileStorageLayer extends StorageLayer {

    private String dirname;

    private String dataFilePath;

    private ObjectMapper objectMapper;

    private DiskDriver diskDriver;

    private int count;

    private long fileSizeThreshold = 1L * 1024 * 1024 * 1024;  // 1GB

    //private long fileSizeThreshold = 1024 * 128 * 1024;  // for unit test
    private int putCount = 0;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public DiskFileStorageLayer(FlushPolicy flushPolicy, String dirname, int flushBlockNumThreshold, int flushTimeThreshold) {
        super(flushPolicy, flushBlockNumThreshold, flushTimeThreshold);
        this.dirname = dirname;
        this.objectMapper = new ObjectMapper();
        this.diskDriver = new DiskDriver(dirname);
        this.count = initCountValue();
        logger.info("init file count value in DISK: [{}]", this.count);
        this.dataFilePath = dirname + File.separator + "trajectory.data." + count;
        this.setStorageLayerName(StorageLayerName.EBS);
    }

    public String printStatus() {
        String status = "[Disk Storage Layer] flushBlockNumThreshold = " + getFlushBlockNumThreshold() + ", flushTimeThreshold" + getFlushTimeThreshold() +
                "\n # of puts (# of chunks): " + putCount;
        return status;
    }

    private int initCountValue() {
        int count = 0;

        File file = new File(dirname);
        String[] filenames = file.list();
        if (filenames == null || filenames.length == 0) {
            return 1;
        }
        for (String filename : filenames) {
            String[] items = filename.split("\\.");
            if (Integer.parseInt(items[items.length-1]) > count) {
                count = Integer.parseInt(items[items.length-1]);
            }
        }

        return count+1;
    }

    /**
     * append this block to the data file
     * @param block
     */
    @Override
    public void put(Block block) {
        putCount++;

        // 1. flush data (as json string currently)
        String blockId = block.getBlockId();
        int fileSizeBefore = diskDriver.getFileSize(dataFilePath);
        try {
            diskDriver.flush(dataFilePath, objectMapper.writeValueAsString(block));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        logger.info("[{}] has been put to DISK file [{}]", blockId, dataFilePath);

        // 2. update mapping table
        int fileSizeAfter = diskDriver.getFileSize(dataFilePath);
        BlockLocation blockLocation = new BlockLocation(StorageLayerName.EBS, dataFilePath, fileSizeBefore, (fileSizeAfter - fileSizeBefore));
        super.updateGlobalLocationMappingTable(blockId, blockLocation);
        super.updateLocalLocationMappingTable(blockId, blockLocation);
        if (fileSizeAfter >= fileSizeThreshold) {
            count = count + 1;
            dataFilePath = dirname + File.separator + "trajectory.data." + count;
        }

        // 3. update meta
        super.updateMetaDataByOne();
    }

    @Override
    public Block get(String blockId) {

        BlockLocation blockLocation = getBlockLocation(blockId);
        if (blockLocation == null) {
            return new Block();
        }
        String blockString = diskDriver.getDataAsStringPartial(blockLocation.getFilepath(), blockLocation.getOffset(), blockLocation.getLength());
        Block block = null;
        logger.info("Get block [{}] from DISK file [file={},off={},len={}]", blockId, blockLocation.getFilepath(), blockLocation.getOffset(), blockLocation.getLength());

        try {
            block = objectMapper.readValue(blockString, Block.class);
            block.setBlockId(blockId);
        } catch (JsonProcessingException e ) {
            e.printStackTrace();
        }

        return block;
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
        for (String blockId : blockIdList) {
            resultBlockList.add(get(blockId));
        }

        return resultBlockList;
    }

    @Override
    public void clearAll() {
        //logger.info("clearAll(): [{}] in DISK", getLocalLocationMappingTable().keySet());
        // 1. clear data
        File file = new File(dirname);
        String[] filenames = file.list();
        for (String filename : filenames) {
            diskDriver.remove(dirname + File.separator + filename);
        }

        // 2. remove mapping in both local and global (when flushing to S3, we should also clear global mapping since we do not record mapping of S3 in Global mapping)
        clearGlobalLocationMappingTable(new ArrayList<>(getLocalLocationMappingTable().keySet()));
        clearLocalLocationMappingTableAll();

        // 3. clear meta
        clearStorageBlockNum();
        logger.info("finish clearAll() of DISK");
    }

    /**
     * TODO need optimization (too much write amplification in some cases)
     * @param blockIdList
     */
    @Override
    public void clear(List<String> blockIdList) {
        //logger.info("clear blocks in DISK: [{}]", blockIdList);
        // 1. clear data
        Set<String> fullFilepathSet = new HashSet<>();
        Map<String, List<String>> blockFileMap = new HashMap<>();  // key is file name, value is the blocks should be remained in this file
        for (String blockId : getLocalLocationMappingTable().keySet()) {
            BlockLocation blockLocation = getLocalLocationMappingTable().get(blockId);
            fullFilepathSet.add(blockLocation.getFilepath());
            if (blockIdList.contains(blockId)) {
                continue;
            }

            if (blockFileMap.containsKey(blockLocation.getFilepath())) {
                blockFileMap.get(blockLocation.getFilepath()).add(blockId);
            } else {
                List<String> blockList = new ArrayList<>();
                blockList.add(blockId);
                blockFileMap.put(blockLocation.getFilepath(), blockList);
            }
        }

        // remove file whose blocks are all needed be deleted
        fullFilepathSet.removeAll(blockFileMap.keySet());
        for (String filepath : fullFilepathSet) {
            diskDriver.remove(filepath);
        }

        for (String filepath : blockFileMap.keySet()) {
            List<String> remainedBlockIdList = blockFileMap.get(filepath);
            count = count + 1;
            String newDataFilePath = dirname + File.separator + "trajectory.data." + count;
            File file = new File(newDataFilePath);
            try {
                if (!file.exists()) {
                    file.createNewFile();
                }
            }catch (IOException e) {
                e.printStackTrace();
            }

            for (String remainedBlockId : remainedBlockIdList) {
                BlockLocation blockLocation = getLocalLocationMappingTable().get(remainedBlockId);
                String blockString = diskDriver.getDataAsStringPartial(filepath, blockLocation.getOffset(), blockLocation.getLength());

                int fileSizeBefore = diskDriver.getFileSize(newDataFilePath);
                diskDriver.flush(newDataFilePath, blockString);
                int fileSizeAfter = diskDriver.getFileSize(newDataFilePath);
                BlockLocation newBlockLocation = new BlockLocation(StorageLayerName.EBS, newDataFilePath, fileSizeBefore, (fileSizeAfter-fileSizeBefore));
                getLocalLocationMappingTable().put(remainedBlockId, newBlockLocation);
                getTieredCloudStorageManager().getBlockLocationMappingTable().put(remainedBlockId, newBlockLocation);
            }

            diskDriver.remove(filepath);
        }



        // 2. clear mapping
        clearLocalLocationMappingTable(blockIdList);
        clearGlobalLocationMappingTable(blockIdList);

        // 3. update meta
        updateStorageBlockNum(getStorageBlockNum() - blockIdList.size());
    }

    @Override
    public void close() {
        diskDriver.close();
    }


}
