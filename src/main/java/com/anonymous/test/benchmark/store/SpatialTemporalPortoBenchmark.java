package com.anonymous.test.benchmark.store;

import com.anonymous.test.benchmark.PortoTaxiRealData;
import com.anonymous.test.benchmark.StatusRecorder;
import com.anonymous.test.common.DimensionNormalizer;
import com.anonymous.test.common.Point;
import com.anonymous.test.common.SpatialBoundingBox;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.SpatialTemporalTree;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.anonymous.test.index.util.IndexConfiguration;
import com.anonymous.test.storage.TieredCloudStorageManager;
import com.anonymous.test.storage.driver.ObjectStoreDriver;
import com.anonymous.test.storage.flush.*;
import com.anonymous.test.storage.layer.DiskFileStorageLayer;
import com.anonymous.test.storage.layer.ImmutableMemoryStorageLayer;
import com.anonymous.test.storage.layer.ObjectStoreStorageLayer;
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
 * @create 2022-06-23 9:27 AM
 **/
public class SpatialTemporalPortoBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        public int objectSize = IngestionForQuery.objectSize;

        public int numOfConnection = IngestionForQuery.numOfConnection;

        @Param({"SPATIO_TEMPORAL"})
        public S3LayoutSchemaName s3LayoutSchemaName;

        public boolean flushToS3 = IngestionForQuery.flushToS3;

        @Param({"86400"})
        public int timeLength;  // unit is s, need to be converted to ms for query predicate

        @Param({"0.001", "0.01", "0.1"})
        public double spatialRange;

        @Param({"10"})
        public int querySetSize;

        List<SpatialTemporalRangeQueryPredicate> predicateList;

        SeriesStore seriesStore;

        TieredCloudStorageManager storageManager;

        ToDiskFlushPolicy toDiskFlushPolicy;

        ToS3FlushPolicy toS3FlushPolicy;

        PortoTaxiRealData portoTaxiRealData;

        @Setup(Level.Trial)
        public void setup() {
            String mybucketName = "";
            if (s3LayoutSchemaName == S3LayoutSchemaName.SPATIO_TEMPORAL) {
                mybucketName = "flush-test-1111-st";
            } else if (s3LayoutSchemaName == S3LayoutSchemaName.SPATIO_TEMPORAL_STR) {
                mybucketName = "flush-test-1111-str";
            }

            // dimension normalizer
            DimensionNormalizer normalizer = new DimensionNormalizer(-180, 180, -90, 90);

            // parameters for in-memory chunk size (the max number of points in a chunk)
            int maxChunkSize = 200;

            // parameters for the index for immutable chunks
            int indexNodeSize = 1024;
            Region region = Region.AP_EAST_1;
            String bucketName = mybucketName;
            String rootDirnameInBucket = "index-test";
            boolean lazyParentUpdate = true;
            boolean preciseSpatialIndex = true;
            boolean enableSpatialIndex = true;
            IndexConfiguration indexConf = new IndexConfiguration(indexNodeSize, lazyParentUpdate, bucketName, rootDirnameInBucket, region, preciseSpatialIndex, enableSpatialIndex);
            SpatialTemporalTree indexForImmutable = new SpatialTemporalTree(indexConf);
            indexForImmutable = indexForImmutable.loadAndRebuildIndex();
            indexForImmutable.setIndexConfiguration(indexConf);

            // parameters for the index for head chunks
            int geoHashShiftLength = 12;
            int postingListCapacity = 50;
            HeadChunkIndexWithGeoHashSemiSplit indexForHead = new HeadChunkIndexWithGeoHashSemiSplit(geoHashShiftLength, normalizer, postingListCapacity);

            // parameters for tiered storage
            String bucketNameFrStorage = mybucketName;
            Region regionForStorage = Region.AP_EAST_1;

            int s3TimePartition = 1000 * 60 * 60 * 24;
            int s3SpatialPartition= 24;
            S3LayoutSchema s3LayoutSchema = new S3LayoutSchema(s3LayoutSchemaName, s3SpatialPartition, s3TimePartition);
            toDiskFlushPolicy = new ToDiskFlushPolicy();
            toS3FlushPolicy = new ToS3FlushPolicy(s3LayoutSchema, objectSize, numOfConnection);

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

            storageManager = new TieredCloudStorageManager(immutableMemoryStorageLayer, diskFileStorageLayer, objectStoreStorageLayer);

            // series store
            seriesStore = new SeriesStore(maxChunkSize, storageManager, indexForImmutable, indexForHead, normalizer, s3LayoutSchema, "from-data");

            List<Integer> randomIndexList = new ArrayList<>();
            Random random = new Random(1);
            for (int i = 0; i < querySetSize; i++) {
                randomIndexList.add(random.nextInt(10000000));
            }

            portoTaxiRealData = new PortoTaxiRealData("/home/anonymous/Data/DataSet/Trajectory/TaxiPorto/archive/porto_data_v1_1000w.csv");
            predicateList = new ArrayList<>();
            TrajectoryPoint point = null;
            int count = 0;
            while ((point = portoTaxiRealData.nextPointFromPortoTaxis()) != null) {
                count++;
                if (count % 1000000 == 0) {
                    System.out.println(count);
                }
                if (randomIndexList.contains(count)) {
                    Point upperRight = new Point(point.getLongitude() + spatialRange, point.getLatitude() + spatialRange);
                    SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(point.getTimestamp(), point.getTimestamp()+timeLength * 1000L, point, upperRight);
                    predicateList.add(predicate);
                }

            }
            System.out.println("bucket name: " + bucketName);
            System.out.println("begin query benchmark");
        }

        @TearDown(Level.Trial)
        public void printStatus() {
            String statusFilename = "/home/anonymous/IdeaProjects/springbok/benchmark-log/storage/status/insertion-status.log";

            if (s3LayoutSchemaName.equals(S3LayoutSchemaName.SPATIO_TEMPORAL)) {
                System.out.println(S3SpatioTemporalLayoutSchemaTool.printStatus());
                StatusRecorder.recordStatus(statusFilename, S3SpatioTemporalLayoutSchemaTool.printStatus());
            } else if (s3LayoutSchemaName.equals(S3LayoutSchemaName.SPATIO_TEMPORAL_STR)) {
                System.out.println(S3SpatioTemporalSTRLayoutSchemaTool.printStatus());
                StatusRecorder.recordStatus(statusFilename, S3SpatioTemporalSTRLayoutSchemaTool.printStatus());
            } else if (s3LayoutSchemaName.equals(S3LayoutSchemaName.SINGLE_TRAJECTORY)) {
                System.out.println(S3SingleTrajectoryLayoutSchemaTool.printStatus());
                StatusRecorder.recordStatus(statusFilename, S3SingleTrajectoryLayoutSchemaTool.printStatus());
            }
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 0, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(10)
    @Measurement(time = 5, iterations = 1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void runQuery(Blackhole blackhole, BenchmarkState state) {
        for (SpatialTemporalRangeQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            List<TrajectoryPoint> result = state.seriesStore.spatialTemporalRangeQueryWithRefinement(predicate.getStartTimestamp(), predicate.getStopTimestamp(), new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight()));

            blackhole.consume(result);
            System.out.println("object store request count: " + ObjectStoreDriver.getCount);
        }

    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SpatialTemporalPortoBenchmark.class.getSimpleName())
                //.output("/home/anonymous/IdeaProjects/springbok/benchmark-log/store/spatialtemporal.log")
                .build();

        new Runner(opt).run();
    }

}
