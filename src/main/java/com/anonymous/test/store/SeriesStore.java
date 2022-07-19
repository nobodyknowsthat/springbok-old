package com.anonymous.test.store;

import com.anonymous.test.common.DimensionNormalizer;
import com.anonymous.test.common.SpatialBoundingBox;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.NodeTuple;
import com.anonymous.test.index.SpatialTemporalTree;
import com.anonymous.test.index.TrajectorySegmentMeta;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.anonymous.test.index.util.IndexConfiguration;
import com.anonymous.test.storage.Block;
import com.anonymous.test.storage.StorageConfiguration;
import com.anonymous.test.storage.StorageLayerName;
import com.anonymous.test.storage.TieredCloudStorageManager;
import com.anonymous.test.storage.flush.S3LayoutSchema;
import com.anonymous.test.storage.layer.StorageLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author anonymous
 * @create 2021-10-04 4:12 PM
 **/
public class SeriesStore {

    private int maxChunkSize;

    private HashMap<String, SingleSeries> seriesStore;

    private ChunkIdManager chunkIdManager;

    private TieredCloudStorageManager tieredCloudStorageManager;

    private SpatialTemporalTree indexForImmutableChunks;

    private HeadChunkIndex indexForHeadChunks;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public SeriesStore(int maxChunkSize, TieredCloudStorageManager tieredCloudStorageManager, SpatialTemporalTree indexForImmutableChunks, HeadChunkIndex headChunkIndex, DimensionNormalizer dimensionNormalizer, S3LayoutSchema s3LayoutSchema, String timestampModeForIdManager) {
        this.maxChunkSize = maxChunkSize;
        this.chunkIdManager = new ChunkIdManager(dimensionNormalizer, s3LayoutSchema, timestampModeForIdManager);
        this.tieredCloudStorageManager = tieredCloudStorageManager;
        this.seriesStore = new HashMap<>();
        this.indexForImmutableChunks = indexForImmutableChunks;
        this.indexForHeadChunks = headChunkIndex;
    }

    /**
     * used for a brand-new store with custom normalizer
     * @param maxChunkSize
     * @param tieredCloudStorageManager
     * @param indexForImmutableChunks
     */
    @Deprecated
    public SeriesStore(int maxChunkSize, TieredCloudStorageManager tieredCloudStorageManager, SpatialTemporalTree indexForImmutableChunks, DimensionNormalizer dimensionNormalizer, HeadChunkIndex headChunkIndex) {
        this.maxChunkSize = maxChunkSize;
        this.chunkIdManager = new ChunkIdManager(dimensionNormalizer);
        this.tieredCloudStorageManager = tieredCloudStorageManager;
        this.seriesStore = new HashMap<>();
        this.indexForImmutableChunks = indexForImmutableChunks;
        this.indexForHeadChunks = headChunkIndex;
    }

    /**
     * used for a brand-new store with custom normalizer and layout schema
     * @param maxChunkSize
     * @param tieredCloudStorageManager
     * @param indexForImmutableChunks
     */
    @Deprecated
    public SeriesStore(int maxChunkSize, TieredCloudStorageManager tieredCloudStorageManager, SpatialTemporalTree indexForImmutableChunks, HeadChunkIndex headChunkIndex, DimensionNormalizer dimensionNormalizer, S3LayoutSchema s3LayoutSchema) {
        this.maxChunkSize = maxChunkSize;
        this.chunkIdManager = new ChunkIdManager(dimensionNormalizer, s3LayoutSchema);
        this.tieredCloudStorageManager = tieredCloudStorageManager;
        this.seriesStore = new HashMap<>();
        this.indexForImmutableChunks = indexForImmutableChunks;
        this.indexForHeadChunks = headChunkIndex;
    }


    /**
     * used for a brand-new store
     * @param maxChunkSize
     * @param tieredCloudStorageManager
     * @param indexForImmutableChunks
     */
    @Deprecated
    public SeriesStore(int maxChunkSize, TieredCloudStorageManager tieredCloudStorageManager, SpatialTemporalTree indexForImmutableChunks) {
        this.maxChunkSize = maxChunkSize;
        this.chunkIdManager = new ChunkIdManager();
        this.tieredCloudStorageManager = tieredCloudStorageManager;
        this.seriesStore = new HashMap<>();
        this.indexForImmutableChunks = indexForImmutableChunks;
    }

    /**
     * used for existed stores that already have indexes
     * @param maxChunkSize
     * @param tieredCloudStorageManager
     * @param indexConfiguration
     */
    @Deprecated
    public SeriesStore(int maxChunkSize, TieredCloudStorageManager tieredCloudStorageManager, IndexConfiguration indexConfiguration) {
        SpatialTemporalTree rebuildIndex = new SpatialTemporalTree(indexConfiguration).loadAndRebuildIndex();
        this.maxChunkSize = maxChunkSize;
        this.chunkIdManager = new ChunkIdManager();
        this.tieredCloudStorageManager = tieredCloudStorageManager;
        this.seriesStore = new HashMap<>();
        this.indexForImmutableChunks = rebuildIndex;
    }

    @Deprecated
    public static SeriesStore initNewStoreWithOptimizedS3FlushForTest() {

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        StorageConfiguration storageConfiguration = TieredCloudStorageManager.getDefaultStorageConfiguration();
        tieredCloudStorageManager.initLayersStructureWithOptimizedS3Flush(storageConfiguration);

        IndexConfiguration indexConfiguration = SpatialTemporalTree.getDefaultIndexConfiguration();
        SpatialTemporalTree spatialTemporalTree = new SpatialTemporalTree(indexConfiguration);

        int maxChunkSize = 1000;
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, tieredCloudStorageManager, spatialTemporalTree);

        return seriesStore;
    }

    @Deprecated
    public static SeriesStore initNewStoreForTest() {

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        StorageConfiguration storageConfiguration = TieredCloudStorageManager.getDefaultStorageConfiguration();
        tieredCloudStorageManager.initLayersStructure(storageConfiguration);

        IndexConfiguration indexConfiguration = SpatialTemporalTree.getDefaultIndexConfiguration();
        SpatialTemporalTree spatialTemporalTree = new SpatialTemporalTree(indexConfiguration);

        int maxChunkSize = 1000;
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, tieredCloudStorageManager, spatialTemporalTree);

        return seriesStore;
    }

    @Deprecated
    public static SeriesStore initNewStoreForInMemTest() {

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        StorageConfiguration storageConfiguration = TieredCloudStorageManager.getDefaultStorageConfiguration();
        tieredCloudStorageManager.initLayersStructureForInMemTest(storageConfiguration);

        SpatialTemporalTree spatialTemporalTree = null;

        int maxChunkSize = Integer.MAX_VALUE;
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, tieredCloudStorageManager, spatialTemporalTree);

        return seriesStore;
    }

    @Deprecated
    public static SeriesStore initExistedStoreForTest() {

        TieredCloudStorageManager tieredCloudStorageManager = new TieredCloudStorageManager();
        StorageConfiguration storageConfiguration = TieredCloudStorageManager.getDefaultStorageConfiguration();
        tieredCloudStorageManager.initLayersStructure(storageConfiguration);

        IndexConfiguration indexConfiguration = SpatialTemporalTree.getDefaultIndexConfiguration();
        SpatialTemporalTree spatialTemporalTree = new SpatialTemporalTree(indexConfiguration).loadAndRebuildIndex();

        int maxChunkSize = 1000;
        SeriesStore seriesStore = new SeriesStore(maxChunkSize, tieredCloudStorageManager, spatialTemporalTree);

        return seriesStore;
    }

    public boolean appendSeriesPoint(TrajectoryPoint point) {

        String sid = point.getOid();

        if (seriesStore.containsKey(sid)) {
            seriesStore.get(sid).appendPoint(point);
        } else {
            // comes a new trajectory series
            SingleSeries series = new SingleSeries(sid, maxChunkSize, tieredCloudStorageManager, chunkIdManager, indexForImmutableChunks, indexForHeadChunks);
            series.appendPoint(point);
            seriesStore.put(sid, series);

        }

        return true;
    }

    public List<Chunk> idTemporalQuery(String oid, long startTime, long stopTime) {
        logger.info("run id temporal query: oid = " + oid + ", startTime = " + startTime + ", stopTime = " + stopTime);
        List<Chunk> resultChunks = new ArrayList<>();

        // 1. check in-memory active part
        if (seriesStore.containsKey(oid)) {
            Chunk headChunk = seriesStore.get(oid).getHeadChunk();
            if (headChunk.getChunk().get(0).getTimestamp() <= stopTime) {
                resultChunks.add(headChunk);
            }
        }

        // 2. check disk/immutable part
        IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(startTime, stopTime, oid);
        List<NodeTuple> resultTupleList = indexForImmutableChunks.searchForIdTemporal(predicate);
        logger.info("index entry from immutable chunk index: " + resultTupleList.size());
        for (NodeTuple tuple : resultTupleList) {
            String blockId = tuple.getBlockId();
            Chunk chunk = Chunk.deserialize(tieredCloudStorageManager.get(blockId).getDataString());
            resultChunks.add(chunk);
        }

        return resultChunks;
    }

    public List<TrajectoryPoint> idTemporalQueryWithRefinement(String oid, long startTime, long stopTime) {
        List<Chunk> chunkList = idTemporalQuery(oid, startTime, stopTime);
        return refineIdTemporalQuery(chunkList, new IdTemporalQueryPredicate(startTime, stopTime, oid));
    }

    public List<Chunk> spatialTemporalRangeQuery(long startTime, long stopTime, SpatialBoundingBox spatialBoundingBox) {
        logger.info("run spatio-temporal range query: startTime = " + startTime + ", stopTime = " + stopTime + ", boundingBox = " + spatialBoundingBox);

        List<Chunk> resultChunks = new ArrayList<>();

        // 1. check in-memory active part
        Set<String> sidSet = indexForHeadChunks.searchForSpatial(spatialBoundingBox);
        logger.info("index entry from head chunk index: " + sidSet.size());
        if (sidSet!= null && !sidSet.isEmpty()) {
            for (String sid : sidSet) {
                if (seriesStore.get(sid).getHeadChunk().getChunk().get(0).getTimestamp() <= stopTime) {
                    resultChunks.add(seriesStore.get(sid).getHeadChunk());
                }
            }
        }

        // 2. check disk/immutable part
        List<NodeTuple> resultTupleList = indexForImmutableChunks.searchForSpatialTemporal(new SpatialTemporalRangeQueryPredicate(startTime, stopTime, spatialBoundingBox.getLowerLeft(), spatialBoundingBox.getUpperRight()));
        logger.info("index entry from immutable chunk index: " + resultTupleList.size());
        System.out.println("index entry from immutable chunk index: " + resultTupleList.size());
        for (NodeTuple tuple : resultTupleList) {
            String blockId = tuple.getBlockId();
            Chunk chunk = Chunk.deserialize(tieredCloudStorageManager.get(blockId).getDataString());
            resultChunks.add(chunk);
        }

        return resultChunks;
    }

    public List<TrajectoryPoint> spatialTemporalRangeQueryWithRefinement(long startTime, long stopTime, SpatialBoundingBox spatialBoundingBox) {
        List<Chunk> chunkList = spatialTemporalRangeQuery(startTime, stopTime, spatialBoundingBox);
        return refineSpatioTemporalQuery(chunkList, new SpatialTemporalRangeQueryPredicate(startTime, stopTime, spatialBoundingBox.getLowerLeft(), spatialBoundingBox.getUpperRight()));
    }

    @Deprecated
    public Set<String> refine(Set<String> sidSet, SpatialBoundingBox boundingBox) {
        Set<String> refinedIdSet = new HashSet<>();

        for (String sid : sidSet) {
            Chunk chunk = seriesStore.get(sid).getHeadChunk();
            boolean mark = false;
            for (TrajectoryPoint point : chunk.getChunk()) {
                if (SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, point)) {
                    mark = true;
                    break;
                }
            }
            if (mark) {
                refinedIdSet.add(sid);
            }

        }
        return refinedIdSet;
    }

    public List<TrajectoryPoint> refineReturnPoints(Set<String> sidSet, SpatialBoundingBox boundingBox) {
        List<TrajectoryPoint> resultPointList = new ArrayList<>();
        for (String sid : sidSet) {
            Chunk chunk = seriesStore.get(sid).getHeadChunk();
            boolean mark = false;
            for (TrajectoryPoint point : chunk.getChunk()) {
                if (SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBox, point)) {
                    resultPointList.add(point);
                }
            }
        }

        return resultPointList;
    }

    public List<TrajectoryPoint> refineIdTemporalQuery(List<Chunk> chunkList, IdTemporalQueryPredicate predicate) {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        int countWithoutRefinement = 0;
        for (Chunk chunk : chunkList) {
            countWithoutRefinement = countWithoutRefinement + chunk.getChunk().size();
            for (TrajectoryPoint point : chunk.getChunk()) {
                if (predicate.getDeviceId().equals(point.getOid()) && predicate.getStartTimestamp() <= point.getTimestamp() && predicate.getStopTimestamp() >= point.getTimestamp()) {
                    pointList.add(point);
                }
            }
        }
        logger.info("record number before refinement: " + countWithoutRefinement + ", record number after refinement: " + pointList.size());

        return pointList;
    }

    public List<TrajectoryPoint> refineSpatioTemporalQuery(List<Chunk> chunkList, SpatialTemporalRangeQueryPredicate predicate) {
        List<TrajectoryPoint> pointList = new ArrayList<>();
        int countWithoutRefinement = 0;
        SpatialBoundingBox boundingBoxPredicate = new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight());
        for (Chunk chunk : chunkList) {
            countWithoutRefinement = countWithoutRefinement + chunk.getChunk().size();
            for (TrajectoryPoint point : chunk.getChunk()) {
                if (SpatialBoundingBox.checkBoundingBoxContainPoint(boundingBoxPredicate, point) && predicate.getStartTimestamp() <= point.getTimestamp() && predicate.getStopTimestamp() >= point.getTimestamp()) {
                    pointList.add(point);
                }
            }
        }
        logger.info("record number before refinement: " + countWithoutRefinement + ", record number after refinement: " + pointList.size());
        System.out.println("record number before refinement: " + countWithoutRefinement + ", record number after refinement: " + pointList.size());
        return pointList;
    }

    private void flush() {

        // 1. put active head chunk to storage
        Set<String> seriesKeys = seriesStore.keySet();
        for (String seriesKey : seriesKeys) {
            SingleSeries series = seriesStore.get(seriesKey);
            Chunk headChunk = series.getHeadChunk();
            headChunk.setChunkId(chunkIdManager.getChunkIdWithTags(headChunk.getSid(), headChunk.getChunk()));
            Block block = new Block(ChunkIdManager.generateStringBlockId(headChunk), Chunk.serialize(headChunk));
            tieredCloudStorageManager.put(block);
            TrajectorySegmentMeta indexEntry = SingleSeries.generateIndexEntry(headChunk);
            indexForImmutableChunks.insert(indexEntry);
        }

    }


    public void stop() {
        // 1. flush all memory data
        flush();

        // 2. close storage layers and index
        indexForImmutableChunks.close();
        tieredCloudStorageManager.close();

    }

    public void flushDataToS3() {
        // 1. flush all memory data
        flush();

        // 2. flush to s3
        for (StorageLayerName storageLayerName : tieredCloudStorageManager.getStorageLayerHierarchyNameList()) {
            StorageLayer storageLayer = tieredCloudStorageManager.getStorageLayerMap().get(storageLayerName);
            storageLayer.flush();
        }
    }

    public void flushDataToDisk() {
        // 1. flush all memory data
        flush();

        // 1. flush to disk
        StorageLayer storageLayer = tieredCloudStorageManager.getStorageLayerMap().get(StorageLayerName.IMMUTABLEMEM);
        storageLayer.flush();

    }

    public SpatialTemporalTree getIndexForImmutableChunks() {
        return indexForImmutableChunks;
    }


    public HashMap<String, SingleSeries> getSeriesStore() {
        return seriesStore;
    }

    public void setChunkIdManager(ChunkIdManager chunkIdManager) {
        this.chunkIdManager = chunkIdManager;
    }
}
