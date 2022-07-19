package com.anonymous.test.store;

import com.anonymous.test.common.Point;
import com.anonymous.test.common.SpatialBoundingBox;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.util.TrajectorySimulator;
import org.junit.Test;

import java.util.List;

public class SeriesMemoryStoreTest {


    @Test
    public void appendSeriesPoint() {
        SeriesStore seriesStore = SeriesStore.initNewStoreForTest();
        for (int i = 0; i < 10; i++) {
            List<TrajectoryPoint> trajectoryPointList = TrajectorySimulator.nextSyntheticTrajectoryPointBatch(3);
            for (TrajectoryPoint point : trajectoryPointList) {
                seriesStore.appendSeriesPoint(point);
            }
        }

        seriesStore.stop();

    }

    @Test
    public void idTemporalQuery() {
        SeriesStore seriesStore = SeriesStore.initExistedStoreForTest();
        List<Chunk> result = seriesStore.idTemporalQuery("device_1", 0, 3);
        System.out.println(result);
    }

    @Test
    public void spatialTemporalQuery() {
        SeriesStore seriesStore = SeriesStore.initExistedStoreForTest();
        SpatialBoundingBox spatialBoundingBox = new SpatialBoundingBox(new Point(0, 0), new Point(3, 3));
        List<Chunk> result = seriesStore.spatialTemporalRangeQuery(0, 3, spatialBoundingBox);
        System.out.println(result);
    }

    @Test
    public void flush() {
    }
}