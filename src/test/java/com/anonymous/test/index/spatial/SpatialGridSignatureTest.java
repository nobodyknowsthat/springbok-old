package com.anonymous.test.index.spatial;

import com.anonymous.test.common.Point;
import com.anonymous.test.common.SpatialBoundingBox;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class SpatialGridSignatureTest {

    @Test
    public void generateSignature() {

        SpatialBoundingBox spatialBoundingBox = new SpatialBoundingBox(new Point(0, 0), new Point(0.1, 0.1));
        List<Point> pointList = new ArrayList<>();
        pointList.add(new Point(0, 0));
        pointList.add(new Point(0, 0.1));
        pointList.add(new Point(0.1, 0));
        pointList.add(new Point(0.1, 0.1));
        pointList.add(new Point(0.04, 0.04));

        SpatialGridSignature.generateSignature(spatialBoundingBox, pointList, 4 ,4);
    }

    @Test
    public void generateSignatureExtremeCase() {
        SpatialBoundingBox spatialBoundingBox = new SpatialBoundingBox(new Point(1, 1), new Point(1, 1));
        List<Point> pointList = new ArrayList<>();
        pointList.add(new Point(1, 1));

        byte[] signature = SpatialGridSignature.generateSignature(spatialBoundingBox, pointList, 4 ,4);

        SpatialBoundingBox predicate = new SpatialBoundingBox(new Point(1, 1), new Point(2, 2));
        boolean result = SpatialGridSignature.checkOverlap(predicate, spatialBoundingBox, signature, 4, 4);
        System.out.println(result);
    }

}