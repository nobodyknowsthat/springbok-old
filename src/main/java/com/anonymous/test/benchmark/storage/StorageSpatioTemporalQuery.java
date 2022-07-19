package com.anonymous.test.benchmark.storage;

import com.anonymous.test.benchmark.SyntheticDataGenerator;
import com.anonymous.test.common.DimensionNormalizer;
import com.anonymous.test.common.Point;
import com.anonymous.test.common.SpatialBoundingBox;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.SpatialTemporalTree;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.anonymous.test.index.util.IndexConfiguration;
import com.anonymous.test.storage.StorageLayerName;
import com.anonymous.test.storage.TieredCloudStorageManager;
import com.anonymous.test.storage.flush.*;
import com.anonymous.test.storage.layer.DiskFileStorageLayer;
import com.anonymous.test.storage.layer.ImmutableMemoryStorageLayer;
import com.anonymous.test.storage.layer.ObjectStoreStorageLayer;
import com.anonymous.test.storage.layer.StorageLayer;
import com.anonymous.test.store.HeadChunkIndexWithGeoHashSemiSplit;
import com.anonymous.test.store.SeriesStore;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author anonymous
 * @create 2022-01-04 8:08 PM
 **/
@Deprecated
public class StorageSpatioTemporalQuery {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"1000"})
        public int objectSize;

        @Param({"4"})
        public int numOfConnection; // more connection can reduce the time (before reaching the bandwidth limit)

        @Param({"100"})
        public int timeLength;  // unit is hour

        @Param({"0.1"})
        public double spatialRange;

        @Param({"10"})
        public int querySetSize;

        @Param({"SPATIO_TEMPORAL"})
        public S3LayoutSchemaName s3LayoutSchemaName;

        @Param({"false"})
        public boolean flushToS3;

        public SeriesStore seriesStore;

        public List<SpatialTemporalRangeQueryPredicate> predicateList;

        TieredCloudStorageManager storageManager;

        ToDiskFlushPolicy toDiskFlushPolicy;

        ToS3FlushPolicy toS3FlushPolicy;

        @Setup(Level.Trial)
        public void setup() {
            // dimension normalizer
            DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

            // parameters for in-memory chunk size (the max number of points in a chunk)
            int maxChunkSize = 200;

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
            //int objectSize = 10000;
            //int numOfConnection = 2;
            int s3TimePartition = 1000 * 60 * 60 * 24;
            int s3SpatialPartition= 24;
            S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(s3LayoutSchemaName, s3SpatialPartition, s3TimePartition);
            toDiskFlushPolicy = new ToDiskFlushPolicy();
            toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

            int flushBlockNumThresholdForMem = 10000;
            int flushTimeThresholdForMem = 1000 * 60 * 60 * 2;
            int flushBlockNumThresholdForDisk = 100000000;
            int flushTimeThresholdForDisk = 1000 * 60 * 60 * 24;

            ImmutableMemoryStorageLayer immutableMemoryStorageLayer = new ImmutableMemoryStorageLayer(toDiskFlushPolicy, flushBlockNumThresholdForMem, flushTimeThresholdForMem);
            DiskFileStorageLayer diskFileStorageLayer = new DiskFileStorageLayer(toS3FlushPolicy,"/home/anonymous/IdeaProjects/springbok/flush-test", flushBlockNumThresholdForDisk, flushTimeThresholdForDisk);
            ObjectStoreStorageLayer objectStoreStorageLayer = new ObjectStoreStorageLayer(null, bucketNameFrStorage, regionForStorage, s3LayoutSchema);

            storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

            // series store
            seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema);

            List<Integer> randomIndexList = new ArrayList<>();
            Random random = new Random(1);
            for (int i = 0; i < querySetSize; i++) {
                randomIndexList.add(random.nextInt(10000000));
            }

            predicateList = new ArrayList<>();
            TrajectoryPoint point = null;
            int count = 0;
            while (true) {
                count++;
                point = SyntheticDataGenerator.nextRandomTrajectoryPoint();
                seriesStore.appendSeriesPoint(point);
                if (count % 1000000 == 0) {
                    System.out.println(count);
                }
                if (randomIndexList.contains(count)) {
                    Point upperRight = new Point(point.getLongitude() + spatialRange, point.getLatitude() + spatialRange);
                    SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(point.getTimestamp(), point.getTimestamp()+timeLength, point, upperRight);
                    predicateList.add(predicate);
                }

                if (count >= 10000000) {
                    break;
                }

            }
            if (flushToS3) {
                seriesStore.flushDataToS3();
            } else {
                seriesStore.flushDataToDisk();
            }
            System.out.println("begin query benchmark");
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            for (StorageLayerName storageLayerName : storageManager.getStorageLayerHierarchyNameList()) {
                StorageLayer storageLayer = storageManager.getStorageLayerMap().get(storageLayerName);
                System.out.println(storageLayer.printStatus());
            }
            System.out.println(toDiskFlushPolicy.printStatus());
            System.out.println(toS3FlushPolicy.printStatus());
            if (s3LayoutSchemaName.equals(S3LayoutSchemaName.SPATIO_TEMPORAL)) {
                System.out.println(S3SpatioTemporalLayoutSchemaTool.printStatus());
            } else if (s3LayoutSchemaName.equals(S3LayoutSchemaName.SPATIO_TEMPORAL_STR)) {
                System.out.println(S3SpatioTemporalSTRLayoutSchemaTool.printStatus());
            } else if (s3LayoutSchemaName.equals(S3LayoutSchemaName.SINGLE_TRAJECTORY)) {
                System.out.println(S3SingleTrajectoryLayoutSchemaTool.printStatus());
            }
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(10)
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void spatioTemporalQuery(Blackhole blackhole, BenchmarkState state) {

        for (SpatialTemporalRangeQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            List<TrajectoryPoint> result = state.seriesStore.spatialTemporalRangeQueryWithRefinement(predicate.getStartTimestamp(), predicate.getStopTimestamp(), new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight()));
            System.out.println(result.size());
            blackhole.consume(result);

        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(StorageSpatioTemporalQuery.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
