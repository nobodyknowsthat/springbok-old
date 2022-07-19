package com.anonymous.test.storage;

import com.anonymous.test.benchmark.SyntheticDataGenerator;
import com.anonymous.test.common.DimensionNormalizer;
import com.anonymous.test.common.Point;
import com.anonymous.test.common.SpatialBoundingBox;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.SpatialTemporalTree;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.anonymous.test.index.util.IndexConfiguration;
import com.anonymous.test.storage.flush.*;
import com.anonymous.test.storage.layer.DiskFileStorageLayer;
import com.anonymous.test.storage.layer.ImmutableMemoryStorageLayer;
import com.anonymous.test.storage.layer.ObjectStoreStorageLayer;
import com.anonymous.test.storage.layer.StorageLayer;
import com.anonymous.test.store.Chunk;
import com.anonymous.test.store.HeadChunkIndexWithGeoHashSemiSplit;
import com.anonymous.test.store.SeriesStore;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangguo
 * @create 2022-01-04 4:35 PM
 **/
public class StorageSTRTest {

    private static List<TrajectoryPoint> syntheticPoints = SyntheticDataGenerator.generateSyntheticPointsForStorageTest(200, 20, 20);

    @Test
    public void insertionTest() {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(0, 20, 0, 20);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 100;

        // parameters for the index for immutable chunks
        int indexNodeSize = 512;
        Region region = Region.AP_EAST_1;
        String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);

        // parameters for the index for head chunks
        int geoHashShiftLength = 20;
        int postingListCapacity = 100;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = "flush-test-1111";
        Region regionForStorage = Region.AP_EAST_1;
        int objectSize = 100;
        int numOfConnection = 2;
        int spatialShiftNum = 36;
        int timePartitionLength = 1000 * 60 * 60 * 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, spatialShiftNum, timePartitionLength);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 100;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk = 500;
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 6;

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, normalizer, indexForHead);

        int count = 0;
        for (TrajectoryPoint point : syntheticPoints) {
            count++;
            seriesStore.appendSeriesPoint(point);
            if (count % 100000 == 0) {
                System.out.println(count);
            }

        }
        seriesStore.stop();

        for (StorageLayerName storageLayerName : storageManager.getStorageLayerHierarchyNameList()) {
            StorageLayer storageLayer = storageManager.getStorageLayerMap().get(storageLayerName);
            System.out.println(storageLayer.printStatus());
        }
        System.out.println(toDiskFlushPolicy.printStatus());
        System.out.println(toS3FlushPolicy.printStatus());
        System.out.println(S3SpatioTemporalSTRLayoutSchemaTool.printStatus());
    }

    @Test
    public void testIdTemporalQuery() {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(0, 20, 0, 20);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 100;

        // parameters for the index for immutable chunks
        int indexNodeSize = 512;
        Region region = Region.AP_EAST_1;
        String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);
        indexForImmutable = indexForImmutable.loadAndRebuildIndex();

        // parameters for the index for head chunks
        int geoHashShiftLength = 20;
        int postingListCapacity = 100;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = "flush-test-1111";
        Region regionForStorage = Region.AP_EAST_1;
        int objectSize = 100;
        int numOfConnection = 2;
        int spatialShiftNum = 36;
        int timePartitionLength = 1000 * 60 * 60 * 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, spatialShiftNum, timePartitionLength);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 100;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk = 500;
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 6;

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, normalizer, indexForHead);

        // query test

        List<IdTemporalQueryPredicate> predicateList = StorageSpatioTemporalTest.generateRandomIdTemporalQueries(50, 66);

        System.out.println("begin query");
        for (IdTemporalQueryPredicate predicate : predicateList) {
            List<Chunk> result = seriesStore.idTemporalQuery(predicate.getDeviceId(), predicate.getStartTimestamp(), predicate.getStopTimestamp());
            System.out.println(result);
            List<TrajectoryPoint> resultPoints = new ArrayList<>();
            for (Chunk chunk : result) {
                resultPoints.addAll(chunk.getChunk());
            }
            StorageSpatioTemporalTest.verifyIdTemporal(predicate, resultPoints, syntheticPoints);
        }
    }

    @Test
    public void testSpatioTemporalQuery() {
        // dimension normalizer
        DimensionNormalizer normalizer = new DimensionNormalizer(0, 20, 0, 20);

        // parameters for in-memory chunk size (the max number of points in a chunk)
        int maxChunkSize = 100;

        // parameters for the index for immutable chunks
        int indexNodeSize = 512;
        Region region = Region.AP_EAST_1;
        String bucketName = "flush-test-1111";
        String rootDirnameInBucket = "index-test";
        boolean lazyParentUpdate = true;
        boolean preciseSpatialIndex = true;
        boolean enableSpatialIndex = true;
        IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
        SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);
        indexForImmutable = indexForImmutable.loadAndRebuildIndex();

        // parameters for the index for head chunks
        int geoHashShiftLength = 20;
        int postingListCapacity = 100;
        HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

        // parameters for tiered storage
        String bucketNameFrStorage = "flush-test-1111";
        Region regionForStorage = Region.AP_EAST_1;
        int objectSize = 100;
        int numOfConnection = 2;
        int spatialShiftNum = 36;
        int timePartitionLength = 1000 * 60 * 60 * 24;
        S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(S3LayoutSchemaName.SPATIO_TEMPORAL_STR, spatialShiftNum, timePartitionLength);
        ToDiskFlushPolicy toDiskFlushPolicy = new ToDiskFlushPolicy();
        ToS3FlushPolicy toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

        int flushBlockNumThresholdForMem = 100;
        int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
        int flushBlockNumThresholdForDisk = 500;
        int flushTimeThresholdForDisk = 1000 * 60 * 60 * 6;

        ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
        DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/yangguo/IdeaProjects/trajectory-index/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
        ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

        TieredCloudStorageManager storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

        // series store
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, normalizer, indexForHead);

        // query test

        // query test
        SpatialTemporalRangeQueryPredicate spatialTemporalRangeQueryPredicate = new SpatialTemporalRangeQueryPredicate(0, 22, new Point(0, 0), new Point(20, 20));
        List<Chunk> fullResult = seriesStore.spatialTemporalRangeQuery(spatialTemporalRangeQueryPredicate.getStartTimestamp(), spatialTemporalRangeQueryPredicate.getStopTimestamp(), new SpatialBoundingBox(spatialTemporalRangeQueryPredicate.getLowerLeft(), spatialTemporalRangeQueryPredicate.getUpperRight()));
        //System.out.println(result);
        List<TrajectoryPoint> resultPointsFull = new ArrayList<>();
        for (Chunk chunk : fullResult) {
            resultPointsFull.addAll(chunk.getChunk());
        }
        StorageSpatioTemporalTest.verifySpatioTemporal(spatialTemporalRangeQueryPredicate, resultPointsFull, syntheticPoints);

        List<SpatialTemporalRangeQueryPredicate> predicateList = StorageSpatioTemporalTest.generateRandomSpatioTemporalQueries(100, 8, 8, 30);
        System.out.println("begin query");
        for (SpatialTemporalRangeQueryPredicate predicate : predicateList) {
            List<Chunk> result = seriesStore.spatialTemporalRangeQuery(predicate.getStartTimestamp(), predicate.getStopTimestamp(), new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight()));
            //System.out.println(result);
            List<TrajectoryPoint> resultPoints = new ArrayList<>();
            for (Chunk chunk : result) {
                resultPoints.addAll(chunk.getChunk());
            }
            StorageSpatioTemporalTest.verifySpatioTemporal(predicate, resultPoints, syntheticPoints);
        }
    }

}
