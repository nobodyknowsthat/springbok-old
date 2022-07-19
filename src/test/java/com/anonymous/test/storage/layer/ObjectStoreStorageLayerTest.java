package com.anonymous.test.storage.layer;

import com.anonymous.test.storage.Block;
import com.anonymous.test.storage.StorageLayerName;
import com.anonymous.test.storage.TieredCloudStorageManager;
import com.anonymous.test.storage.flush.S3LayoutSchema;
import com.anonymous.test.storage.flush.S3LayoutSchemaName;
import com.anonymous.test.util.BlockGenerator;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;

public class ObjectStoreStorageLayerTest {

    private TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();

    @Test
    public void put() {

        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, null);

        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.S3, objectStoreStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.S3);

        objectStoreStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        for (int i = 0; i < 32; i++) {
            objectStoreStorageLayer.put(BlockGenerator.generateNextBlock());
        }

        System.out.println();

    }

    @Test
    public void get() {
        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null,  bucketName, region, new S3LayoutSchema(S3LayoutSchemaName.DIRECT));

        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.S3, objectStoreStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.S3);

        objectStoreStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);
        Block block = objectStoreStorageLayer.get("E000.4");

        System.out.println(block);

        System.out.println();
    }

    @Test
    public void batchPut() {
        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, null);

        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.S3, objectStoreStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.S3);

        objectStoreStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);

        List<Block> blockList = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            blockList.add(BlockGenerator.generateNextBlock());
        }
        objectStoreStorageLayer.batchPut(blockList);

        List<String> blockIdList = new ArrayList<>();
        blockIdList.add("T000.15");
        blockIdList.add("T000.2");

        List<Block> blocks = objectStoreStorageLayer.batchGet(blockIdList);

        System.out.println(blocks);

        System.out.println();


    }

    @Test
    public void batchGet() {
        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, new S3LayoutSchema(S3LayoutSchemaName.DIRECT));

        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.S3, objectStoreStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.S3);

        objectStoreStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);

        List<String> blockIdList = new ArrayList<>();
        blockIdList.add("T000.1");
        blockIdList.add("T000.2");

        List<Block> blocks = objectStoreStorageLayer.batchGet(blockIdList);

        System.out.println(blocks);

        System.out.println();
    }

    @Test
    public void clearAll() {
        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, null);

        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.S3, objectStoreStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.S3);

        objectStoreStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);


        objectStoreStorageLayer.clearAll();
    }

    @Test
    public void clear() {

        String bucketName = "flush-test-1111";
        Region region = Region.AP_EAST_1;
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketName, region, null);

        tieredCloudStorageManager.getStorageLayerMap().put(StorageLayerName.S3, objectStoreStorageLayer);
        tieredCloudStorageManager.getStorageLayerHierarchyNameList().add(StorageLayerName.S3);

        objectStoreStorageLayer.setTieredCloudStorageManager(tieredCloudStorageManager);

        List<String> blockIdList = new ArrayList<>();
        blockIdList.add("T000.1");
        blockIdList.add("T000.2");

        objectStoreStorageLayer.clear(blockIdList);

    }
}