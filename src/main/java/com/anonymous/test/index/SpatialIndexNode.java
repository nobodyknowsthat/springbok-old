package com.anonymous.test.index;

import com.anonymous.test.index.predicate.BasicQueryPredicate;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.anonymous.test.common.Point;
import com.anonymous.test.common.SpatialBoundingBox;
import com.anonymous.test.index.spatial.SpatialGridSignature;
import com.anonymous.test.index.spatial.TwoLevelGridIndex;
import com.anonymous.test.common.TrajectoryPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Date 2021/3/15 14:51
 * @Created by anonymous
 */
public class SpatialIndexNode extends TreeNode {

    List<SpatialIndexNodeTuple> tuples;

    private static Logger logger = LoggerFactory.getLogger(SpatialIndexNode.class);

    public SpatialIndexNode(TreeNode parentNode, SpatialTemporalTree indexTree) {
        super(parentNode, indexTree);
        this.tuples = new ArrayList<SpatialIndexNodeTuple>();
    }

    public SpatialIndexNode() {}

    public boolean insert(NodeTuple tuple) {
        return true;
    }

    /**
     * search tuples according to spatial information
     * @param meta
     * @return
     */
    public List<NodeTuple> search(BasicQueryPredicate meta) {

        List<NodeTuple> resultTuples = new ArrayList<NodeTuple>();

        SpatialTemporalRangeQueryPredicate spatialTemporalPredicate = null;
        if (meta instanceof SpatialTemporalRangeQueryPredicate) {
            spatialTemporalPredicate = (SpatialTemporalRangeQueryPredicate) meta;
        } else {
            return resultTuples;
        }

        SpatialBoundingBox predicateBox = new SpatialBoundingBox(spatialTemporalPredicate.getLowerLeft(), spatialTemporalPredicate.getUpperRight());

        if (getIndexTree().getIndexConfiguration().isUsePreciseSpatialIndex()) {
            /*for (SpatialIndexNodeTuple indexNodeTuple : tuples) {
                if (isIntersected(indexNodeTuple, spatialTemporalPredicate)) {
                    resultTuples.add(indexNodeTuple);
                }
            }*/
            for (SpatialIndexNodeTuple mbrTuple : tuples) {
                //SpatialIndexNodeMBRTuple mbrTuple = (SpatialIndexNodeMBRTuple) indexNodeTuple;
                if (SpatialBoundingBox.getOverlappedBoundingBox(mbrTuple.getBoundingBox(), predicateBox) != null
                        && SpatialGridSignature.checkOverlap(predicateBox, mbrTuple.getBoundingBox(), mbrTuple.getSignature(), 4, 4)
                        && mbrTuple.getStartTimestamp() <= spatialTemporalPredicate.getStopTimestamp()
                        && mbrTuple.getStopTimestamp() >= spatialTemporalPredicate.getStartTimestamp()) {
                    resultTuples.add(mbrTuple);
                }
            }
        } else {
            // use mbr directly
            for (SpatialIndexNodeTuple mbrTuple : tuples) {
                //SpatialIndexNodeMBRTuple mbrTuple = (SpatialIndexNodeMBRTuple) indexNodeTuple;
                if (SpatialBoundingBox.getOverlappedBoundingBox(mbrTuple.getBoundingBox(), predicateBox) != null
                && mbrTuple.getStartTimestamp() <= spatialTemporalPredicate.getStopTimestamp()
                && mbrTuple.getStopTimestamp() >= spatialTemporalPredicate.getStartTimestamp()) {
                    resultTuples.add(mbrTuple);
                }
            }
        }

        return resultTuples;
    }

    public boolean insert(TrajectorySegmentMeta meta) {
        //TwoLevelGridIndex spatialIndex = this.getIndexTree().getSpatialTwoLevelGridIndex();
        if (getIndexTree().getIndexConfiguration().isUsePreciseSpatialIndex()) {

            /*Set<Long> gridIdList = spatialIndex.toPreciseIndexGridsOptimized(meta.getTrajectoryPointList());
            for (Long gridId: gridIdList) {
                SpatialIndexNodeTuple spatialIndexNodeTuple = new SpatialIndexNodeTuple(meta.getBlockId(), gridId, meta.getDeviceId());
                spatialIndexNodeTuple.setNodeType(NodeType.DATA);
                tuples.add(spatialIndexNodeTuple);
                this.getIndexTree().spatialEntryNum++;
            }*/
            SpatialBoundingBox boundingBox = new SpatialBoundingBox(meta.getLowerLeft(), meta.getUpperRight());
            byte[] signature = SpatialGridSignature.generateSignature(boundingBox, meta.getTrajectoryPointList(),4, 4);
            SpatialIndexNodeTuple mbrTuple = new SpatialIndexNodeTuple(meta.getBlockId(), meta.getDeviceId(), boundingBox, meta.getStartTimestamp(), meta.getStopTimestamp(), signature);
            mbrTuple.setNodeType(NodeType.DATA);
            tuples.add(mbrTuple);
            this.getIndexTree().spatialEntryNum++;

        } else {
            //gridIdList = spatialIndex.toIndexGrids(new SpatialBoundingBox(meta.getLowerLeft(), meta.getUpperRight()));
            SpatialBoundingBox boundingBox = new SpatialBoundingBox(meta.getLowerLeft(), meta.getUpperRight());
            SpatialIndexNodeTuple mbrTuple = new SpatialIndexNodeTuple(meta.getBlockId(), meta.getDeviceId(), boundingBox, meta.getStartTimestamp(), meta.getStopTimestamp());
            mbrTuple.setNodeType(NodeType.DATA);
            tuples.add(mbrTuple);
            this.getIndexTree().spatialEntryNum++;
        }


        return true;
    }

    private static List<Point> transfer2PointList(List<TrajectoryPoint> trajectoryPointList) {
        List<Point> pointList = new ArrayList<>();
        for (TrajectoryPoint trajectoryPoint : trajectoryPointList) {
            Point point = new Point(trajectoryPoint.getLongitude(), trajectoryPoint.getLatitude());
            pointList.add(point);
        }
        return pointList;
    }


    private boolean isIntersected(SpatialIndexNodeTuple tuple, SpatialTemporalRangeQueryPredicate predicate) {

        long spatialGridId = tuple.getSpatialGridId();
        SpatialBoundingBox predicateBoundingBox = new SpatialBoundingBox(predicate.getLowerLeft(), predicate.getUpperRight());

        TwoLevelGridIndex spatialIndex = this.getIndexTree().getSpatialTwoLevelGridIndex();
        SpatialBoundingBox tupleBoundingBox = spatialIndex.getBoundingBoxByGridId(spatialGridId);
        SpatialBoundingBox result = spatialIndex.getOverlappedBoundingBox(predicateBoundingBox, tupleBoundingBox);

        if (result == null) {
            return false;
        }

        return true;
    }

    public List<SpatialIndexNodeTuple> getTuples() {
        return tuples;
    }

    public void setTuples(List<SpatialIndexNodeTuple> tuples) {
        this.tuples = tuples;
    }

    @Override
    public String toString() {
        return tuples + "\n ";
    }
}
