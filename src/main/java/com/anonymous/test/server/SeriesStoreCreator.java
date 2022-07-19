package com.anonymous.test.server;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.common.DimensionNormalizer;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.SpatialTemporalTree;
import com.anonymous.test.index.recovery.LeafNodeStatusRecorder;
import com.anonymous.test.index.util.IndexConfiguration;
import com.anonymous.test.storage.TieredCloudStorageManager;
import com.anonymous.test.storage.flush.S3LayoutSchema;
import com.anonymous.test.storage.flush.S3LayoutSchemaName;
import com.anonymous.test.storage.flush.ToDiskFlushPolicy;
import com.anonymous.test.storage.flush.ToS3FlushPolicy;
import com.anonymous.test.storage.layer.DiskFileStorageLayer;
import com.anonymous.test.storage.layer.ImmutableMemoryStorageLayer;
import com.anonymous.test.storage.layer.ObjectStoreStorageLayer;
import com.anonymous.test.store.HeadChunkIndexWithGeoHashSemiSplit;
import com.anonymous.test.store.SeriesStore;
import software.amazon.awssdk.regions.Region;

/**
 * @author anonymous
 * @create 2022-06-25 4:21 PM
 **/
public class SeriesStoreCreator {

    private static int objectSize = 2000;

    private static int numOfConnection = 1;

    static SeriesStore createEmptySeriesStore(S3LayoutSchemaName s3LayoutSchemaName, String bucketName, boolean flushToS3) {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 200;

        // parameters for the index for immutable chunks
        int indexNodeSize = 1024;
        Region region = Region.AP_EAST_1;
        //String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);

        // parameters for the index for head chunks
        int geoHashShiftLength = 12;
        int postingListCapacity = 50;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = bucketName;
        Region regionForStorage = Region.AP_EAST_1;

        int s3TimePartition = 1000 * 60 * 60 * 24;
        int s3SpatialPartition= 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(s3LayoutSchemaName, s3SpatialPartition, s3TimePartition);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 10000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk;
        if (flushToS3) {
            flushBlockNumThresholdForDisk = 100000;
        } else {
            flushBlockNumThresholdForDisk = 1000000000;
        }
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 24;
        System.out.println("disk flush threshold: " + flushBlockNumThresholdForDisk);

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/anonymous/IdeaProjects/springbok/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, "from-data");



        return seriesStore;
    }

    static SeriesStore createEmptySeriesStoreWithRecoveryAndDiskFlush(S3LayoutSchemaName s3LayoutSchemaName, String bucketName, LeafNodeStatusRecorder recorder) {
        boolean flushToS3 = true;

        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 200;

        // parameters for the index for immutable chunks
        int indexNodeSize = 1024;
        Region region = Region.AP_EAST_1;
        //String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex, true);
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);
        indexForImmutable.setLeafNodeStatusRecorder(recorder);

        // parameters for the index for head chunks
        int geoHashShiftLength = 12;
        int postingListCapacity = 50;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = bucketName;
        Region regionForStorage = Region.AP_EAST_1;

        int s3TimePartition = 1000 * 60 * 60 * 24;
        int s3SpatialPartition= 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(s3LayoutSchemaName, s3SpatialPartition, s3TimePartition);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        toDiskFlushPolicy.setLeafNodeStatusRecorder(recorder);

        int flushBlockNumThresholdForMem = 10000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk;
        if (flushToS3) {
            flushBlockNumThresholdForDisk = 100000;
        } else {
            flushBlockNumThresholdForDisk = 1000000000;
        }
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 24;
        System.out.println("disk flush threshold: " + flushBlockNumThresholdForDisk);

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/anonymous/IdeaProjects/springbok/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, "from-data");

        return seriesStore;
    }

    static SeriesStore createAndFillSeriesStore(String dataFile, S3LayoutSchemaName s3LayoutSchemaName, String bucketName, boolean flushToS3) {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 200;

        // parameters for the index for immutable chunks
        int indexNodeSize = 1024;
        Region region = Region.AP_EAST_1;
        //String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);

        // parameters for the index for head chunks
        int geoHashShiftLength = 12;
        int postingListCapacity = 50;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = bucketName;
        Region regionForStorage = Region.AP_EAST_1;

        int s3TimePartition = 1000 * 60 * 60 * 24;
        int s3SpatialPartition= 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(s3LayoutSchemaName, s3SpatialPartition, s3TimePartition);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 10000;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk;
        if (flushToS3) {
            flushBlockNumThresholdForDisk = 100000;
        } else {
            flushBlockNumThresholdForDisk = 1000000000;
        }
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 24;
        System.out.println("disk flush threshold: " + flushBlockNumThresholdForDisk);

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/anonymous/IdeaProjects/springbok/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, "from-data");

        PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(dataFile);

        TrajectoryPoint point = null;
        int count = 0;
        while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
            count++;
            seriesStore.appendSeriesPoint(point);
            if (count % 1000000 == 0) {
                System.out.println(count);
            }

        }
        /*if (flushToS3) {
            seriesStore.flushDataToS3();
        } else {
            seriesStore.flushDataToDisk();
        }*/
        /*indexForImmutable.serializeAndFlushIndex();
        System.out.println("finish insertion");*/
        System.out.println("finish creation and filling");

        return seriesStore;
    }

}
