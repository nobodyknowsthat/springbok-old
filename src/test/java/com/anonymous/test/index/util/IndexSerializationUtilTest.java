package com.anonymous.test.index.util;

import com.anonymous.test.common.Point;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.index.SpatialIndexNode;
import com.anonymous.test.index.SpatialTemporalTree;
import com.anonymous.test.index.TrajectorySegmentMeta;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class IndexSerializationUtilTest {

    @Test
    public void serializeLeafSpatialNode() {

        SpatialTemporalTree tree = new SpatialTemporalTree(SpatialTemporalTree.getDefaultIndexConfiguration());
        SpatialIndexNode spatialIndexNode = new SpatialIndexNode(null, tree);
        List<TrajectoryPoint> pointList = new ArrayList<>();
        pointList.add(new TrajectoryPoint("test", 0, 0, 0));
        pointList.add(new TrajectoryPoint("test", 12, 1, 1));
        spatialIndexNode.insert(new TrajectorySegmentMeta(0, 12, new Point(0, 0), new Point(1, 2), "test", "1", pointList));
        String result = IndexSerializationUtil.serializeLeafSpatialNode(spatialIndexNode);
        System.out.println(result);
        SpatialIndexNode reback = IndexSerializationUtil.deserializeLeafSpatialNode(result);
        System.out.println(reback);
    }

}