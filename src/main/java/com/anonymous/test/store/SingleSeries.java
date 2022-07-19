package com.anonymous.test.store;

import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.SpatialTemporalTree;
import com.anonymous.test.index.TrajectorySegmentMeta;
import com.anonymous.test.common.Point;
import com.anonymous.test.common.SpatialBoundingBox;
import com.anonymous.test.storage.Block;
import com.anonymous.test.storage.TieredCloudStorageManager;

import java.util.List;

/**
 * @Description
 * @Date 2021/4/25 10:02
 * @Created by anonymous
 */
public class SingleSeries {

    private String sid;   // series id (usually use oid of trajectory)

    private Chunk headChunk;    // the only active chunk that receive trajectory points

    private int maxChunkSize;    // the max point count of head chunk

    private TieredCloudStorageManager tieredCloudStorageManager;  // archived chunk will be put into immutable region

    private ChunkIdManager chunkIdManager;    // manage chunk id of series

    private SpatialTemporalTree indexForImmutableChunks;

    private HeadChunkIndex indexForHeadChunks;

    public SingleSeries(String sid, int maxChunkSize, TieredCloudStorageManager tieredCloudStorageManager, ChunkIdManager chunkIdManager, SpatialTemporalTree indexForImmutableChunks, HeadChunkIndex indexForHeadChunks) {
        //String chunkId = String.valueOf(chunkIdManager.getChunkId(sid));
        this.headChunk = new Chunk(sid);
        this.sid = sid;
        this.maxChunkSize = maxChunkSize;
        this.chunkIdManager = chunkIdManager;
        this.tieredCloudStorageManager = tieredCloudStorageManager;
        this.indexForImmutableChunks = indexForImmutableChunks;
        this.indexForHeadChunks = indexForHeadChunks;
    }


    public void appendPoint(TrajectoryPoint point) {


        if (headChunk.size() < maxChunkSize) {

            headChunk.add(point);
            // update index for head chunk
            if (indexForHeadChunks != null) {
                indexForHeadChunks.updateIndex(point);
            }

        } else {
            // move this chunk to immutable memory and create a new chunk
            headChunk.setChunkId(chunkIdManager.getChunkIdWithTags(sid, headChunk.getChunk()));
            Block block = new Block(ChunkIdManager.generateStringBlockId(headChunk), Chunk.serialize(headChunk));
            tieredCloudStorageManager.put(block);
            if (indexForHeadChunks != null) {
                indexForHeadChunks.removeFromIndex(headChunk);
            }
            // update index tree for chunks
            TrajectorySegmentMeta indexEntry = generateIndexEntry(headChunk);
            if (indexForImmutableChunks != null) {
                indexForImmutableChunks.insert(indexEntry);
            }

            //String chunkId = String.valueOf(chunkIdManager.getChunkId(sid));
            headChunk = new Chunk(sid);
            headChunk.add(point);
            if (indexForHeadChunks != null) {
                indexForHeadChunks.updateIndex(point);
            }
        }

    }


    private SpatialBoundingBox generateSpatialBoundingBox(Chunk chunk) {
        List<TrajectoryPoint> trajectoryPointList = chunk.getChunk();
        double lonMin = Double.MAX_VALUE;
        double lonMax = -Double.MAX_VALUE;
        double latMin = Double.MAX_VALUE;
        double latMax = -Double.MAX_VALUE;

        for (TrajectoryPoint point : trajectoryPointList) {
            if (point.getLongitude() < lonMin) {
                lonMin = point.getLongitude();
            }
            if (point.getLongitude() > lonMax) {
                lonMax = point.getLongitude();
            }
            if (point.getLatitude() < latMin) {
                latMin = point.getLatitude();
            }
            if (point.getLatitude() > latMax) {
                latMax = point.getLatitude();
            }
        }

        return new SpatialBoundingBox(new Point(lonMin, latMin), new Point(lonMax, latMax));
    }

    public static TrajectorySegmentMeta generateIndexEntry(Chunk chunk) {

        List<TrajectoryPoint> trajectoryPointList = chunk.getChunk();
        long startTime = Long.MAX_VALUE;
        long stopTime = Long.MIN_VALUE;
        double lonMin = Double.MAX_VALUE;
        double lonMax = -Double.MAX_VALUE;
        double latMin = Double.MAX_VALUE;
        double latMax = -Double.MAX_VALUE;

        for (TrajectoryPoint point : trajectoryPointList) {
            if (point.getTimestamp() < startTime) {
                startTime = point.getTimestamp();
            }
            if (point.getTimestamp() > stopTime) {
                stopTime = point.getTimestamp();
            }
            if (point.getLongitude() < lonMin) {
                lonMin = point.getLongitude();
            }
            if (point.getLongitude() > lonMax) {
                lonMax = point.getLongitude();
            }
            if (point.getLatitude() < latMin) {
                latMin = point.getLatitude();
            }
            if (point.getLatitude() > latMax) {
                latMax = point.getLatitude();
            }
        }

        String blockId = ChunkIdManager.generateStringBlockId(chunk);
        TrajectorySegmentMeta indexEntry = new TrajectorySegmentMeta(startTime, stopTime, new Point(lonMin, latMin), new Point(lonMax, latMax), chunk.getSid(), blockId, trajectoryPointList);
        return indexEntry;
    }

    /*public static String generateBlockId(Chunk chunk) {
        return chunk.getSid() + "." + chunk.getChunkId();
    }*/

    public Chunk getHeadChunk() {
        return headChunk;
    }

    @Override
    public String toString() {
        return "SingleSeries{" +
                "sid='" + sid + '\'' +
                ", headChunk=" + headChunk +
                '}';
    }
}
