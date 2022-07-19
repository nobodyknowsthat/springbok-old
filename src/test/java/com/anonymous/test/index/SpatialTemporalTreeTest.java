package com.anonymous.test.index;

import com.anonymous.test.common.Point;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.anonymous.test.index.spatial.TwoLevelGridIndex;
import com.anonymous.test.index.util.IndexConfiguration;
import com.anonymous.test.index.util.IndexTupleGenerator;
import com.anonymous.test.index.util.TreePrinter;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class SpatialTemporalTreeTest {

    @Test
    public void generateBlockId() {
        SpatialTemporalTree tree = new SpatialTemporalTree(3);
        System.out.println(tree.generateBlockId());
    }

    @Test
    public void insert() {
        SpatialTemporalTree tree = generateTestIndexTreeUsingSyntheticData();
        new TreePrinter(tree).print(System.out);
    }

    @Test
    public void searchForIdTemporal() {
        SpatialTemporalTree tree = generateTestIndexTreeUsingConf();
        new TreePrinter(tree).print(System.out);
        List<NodeTuple> results = tree.searchForIdTemporal(new IdTemporalQueryPredicate(0,3, "1"));
        System.out.println("\nquery result: ");
        System.out.println(results);
    }

    @Test
    public void searchForSpatialTemporal() {
        SpatialTemporalTree tree = generateTestIndexTreeUsingSyntheticData();
        //new TreePrinter(tree).print(System.out);
        List<NodeTuple> results = tree.searchForSpatialTemporal(new SpatialTemporalRangeQueryPredicate(0, 3, new Point(0, 0), new Point(1.5, 1.5)));
        System.out.println("\nquery result: ");
        System.out.println(results);
    }

    public static SpatialTemporalTree generateTestIndexTree() {
        SpatialTemporalTree spatialTemporalTree = new SpatialTemporalTree(4);

        List<TrajectorySegmentMeta> metaList = IndexTupleGenerator.generateRandomTupleForIndexTest(4, 10);
        long start = System.currentTimeMillis();
        for (TrajectorySegmentMeta meta : metaList) {
            spatialTemporalTree.insert(meta);
        }
        long stop = System.currentTimeMillis();
        System.out.println("insertion time: " + (stop - start));

        return spatialTemporalTree;

    }

    public static SpatialTemporalTree generateTestIndexTreeUsingConf() {
        IndexConfiguration indexConfiguration = new IndexConfiguration();
        indexConfiguration.setBlockSize(4);
        indexConfiguration.setLazyParentUpdateForActiveNode(true);
        SpatialTemporalTree spatialTemporalTree = new SpatialTemporalTree(indexConfiguration, new TwoLevelGridIndex());

        List<TrajectorySegmentMeta> metaList = IndexTupleGenerator.generateRandomTupleForIndexTest(4, 10);
        long start = System.currentTimeMillis();
        for (TrajectorySegmentMeta meta : metaList) {
            spatialTemporalTree.insert(meta);
        }
        long stop = System.currentTimeMillis();
        System.out.println("insertion time: " + (stop - start));

        return spatialTemporalTree;

    }

    public static SpatialTemporalTree generateTestIndexTreeUsingSyntheticData() {
        SpatialTemporalTree spatialTemporalTree = new SpatialTemporalTree(2);

        List<TrajectorySegmentMeta> metaList = IndexTupleGenerator.generateSyntheticTupleForIndexTest(15, 1, 10);
        //System.out.println(metaList);
        long start = System.currentTimeMillis();
        for (TrajectorySegmentMeta meta : metaList) {
            spatialTemporalTree.insert(meta);
        }
        long stop = System.currentTimeMillis();
        System.out.println("insertion time: " + (stop - start));

        return spatialTemporalTree;

    }
}