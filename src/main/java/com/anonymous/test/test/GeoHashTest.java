package com.anonymous.test.test;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.anonymous.test.benchmark.SyntheticDataGenerator;
import com.anonymous.test.common.TrajectoryPoint;
import com.anonymous.test.store.HeadChunkIndexWithGeoHash;

import java.util.List;

/**
 * @author anonymous
 * @create 2021-12-16 9:33 PM
 **/
public class GeoHashTest {

    public static void main(String[] args) {
        List<TrajectoryPoint> pointList = SyntheticDataGenerator.generateRandomDistributedDataset(1000000, 10, 10);
        HeadChunkIndexWithGeoHash indexWithGeoHash = new HeadChunkIndexWithGeoHash();
        long total = 0;
        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();

            for (TrajectoryPoint point : pointList) {
                indexWithGeoHash.updateIndex(point);
            }
            long stop = System.currentTimeMillis();
            System.out.println("time: " + (stop - start));
            total = total + (stop - start);
        }
        System.out.println("average: " + (total / 100.0));

    }

}
