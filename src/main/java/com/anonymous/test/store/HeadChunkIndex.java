package com.anonymous.test.store;

import com.anonymous.test.common.SpatialBoundingBox;
import com.anonymous.test.common.TrajectoryPoint;

import java.util.Set;

public interface HeadChunkIndex {

    void updateIndex(TrajectoryPoint point);

    void removeFromIndex(Chunk headChunk);

    Set<String> searchForSpatial(SpatialBoundingBox spatialBoundingBox);

    String printStatus();

}
