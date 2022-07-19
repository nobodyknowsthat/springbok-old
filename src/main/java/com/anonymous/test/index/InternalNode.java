package com.anonymous.test.index;


import com.anonymous.test.index.predicate.BasicQueryPredicate;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Date 2021/3/15 15:16
 * @Created by anonymous
 */
public class InternalNode extends TreeNode {

    private List<InternalNodeTuple>  tuples;

    /**
     * only used for rebuild
     */
    public InternalNode() {
    }

    public InternalNode(TreeNode parentNode, SpatialTemporalTree indexTree) {
        super(parentNode, indexTree);
        this.tuples = new ArrayList<InternalNodeTuple>();
        this.setBlockId(String.valueOf(this.getIndexTree().generateBlockId()));
    }

    public List<NodeTuple> search(BasicQueryPredicate meta) {
        List<NodeTuple> resultTuples = new ArrayList<NodeTuple>();
        long startTimePredicate = meta.getStartTimestamp();
        long stopTimePredicate = meta.getStopTimestamp();
        for (InternalNodeTuple tuple : this.getTuples()) {
            if (tuple.getStartTimestamp() <= stopTimePredicate && tuple.getStopTimestamp() >= startTimePredicate) {
                resultTuples.add(tuple);
            }
        }

        return resultTuples;
    }

    @Override
    List<NodeTuple> searchForIdTemporal(IdTemporalQueryPredicate predicate) {
        // there is only temporal information in the internal node
        return search(predicate);
    }

    @Override
    List<NodeTuple> searchForSpatialTemporal(SpatialTemporalRangeQueryPredicate predicate) {
        // there is only temporal information in the internal node
        return search(predicate);
    }

    public boolean insert(TrajectorySegmentMeta meta) {
        return true;
    }

    /**
     *
     * @param tuple
     * @param nodeOfThisTuple  the node pointed by this tuple
     * @return
     */
   /* @Deprecated
    public boolean insert(NodeTuple tuple, TreeNode nodeOfThisTuple) {

        if (tuples.size() < this.getIndexTree().getBlockSize()) {
            tuples.add((InternalNodeTuple) tuple);

            // update the meta data in the internal node
            updateMetaInInternalNode((InternalNodeTuple) tuple);
        } else {
            // this node is full
            InternalNode newInternalNode = new InternalNode(this.getParentNode(), this.getIndexTree());
            newInternalNode.insert(tuple, nodeOfThisTuple);
            nodeOfThisTuple.setParentNode(newInternalNode);    // update its child's parent node

            NodeTuple newInternalNodeTuple = generateInternalNodeTuple(newInternalNode);

            if (newInternalNode.getParentNode() != null) {
                newInternalNode.getParentNode().insert(newInternalNodeTuple, newInternalNode);
            } else {

                // create new root node
                TreeNode newRootNode = new InternalNode(null, this.getIndexTree());
                newRootNode.insert(generateInternalNodeTuple(this), this);
                newRootNode.insert(generateInternalNodeTuple(newInternalNode), newInternalNode);
                this.setParentNode(newRootNode);
                newInternalNode.setParentNode(newRootNode);
                this.getIndexTree().setRoot(newRootNode);
                this.getIndexTree().setRootNodeBlockId(newRootNode.getBlockId());
                this.getIndexTree().setRootType(NodeType.INTERNAL);

            }

        }

        return true;
    }*/

    /**
     * should be equal to public boolean insert(NodeTuple tuple, TreeNode nodeOfThisTuple)
     * since tuple contains the node pointer points to the tuple
     * @param tuple
     * @return
     */
    public boolean insert(NodeTuple tuple) {

        if (tuples.size() < this.getIndexTree().getBlockSize()) {
            tuples.add((InternalNodeTuple) tuple);

            // update the meta data in the internal node
            updateMetaInInternalNode((InternalNodeTuple) tuple);
        } else {
            // this node is full
            InternalNode newInternalNode = new InternalNode(this.getParentNode(), this.getIndexTree());
            newInternalNode.insert(tuple);
            ((InternalNodeTuple)tuple).getNodePointer().setParentNode(newInternalNode);    // update its child's parent node

            NodeTuple newInternalNodeTuple = generateInternalNodeTuple(newInternalNode);

            if (newInternalNode.getParentNode() != null) {
                newInternalNode.getParentNode().insert(newInternalNodeTuple);
            } else {

                // create new root node
                InternalNode newRootNode = new InternalNode(null, this.getIndexTree());
                newRootNode.insert(generateInternalNodeTuple(this));
                newRootNode.insert(generateInternalNodeTuple(newInternalNode));
                this.setParentNode(newRootNode);
                newInternalNode.setParentNode(newRootNode);
                this.getIndexTree().setRoot(newRootNode);
                this.getIndexTree().setHeight(this.getIndexTree().getHeight()+1);

            }

        }

        return true;
    }


    private void updateMetaInInternalNode(InternalNodeTuple insertedTuple) {
        TreeNode currentNode = this;
        TreeNode parentNode = currentNode.getParentNode();
        while (parentNode != null) {

            InternalNode internalNode = (InternalNode) parentNode;
            for (InternalNodeTuple tuple : internalNode.getTuples()) {
                if (tuple.getBlockId().equals(currentNode.getBlockId())) {
                    if (tuple.getStopTimestamp() < insertedTuple.getStopTimestamp()) {
                        tuple.setStopTimestamp(insertedTuple.getStopTimestamp());
                    }
                    if (tuple.getStartTimestamp() > insertedTuple.getStartTimestamp()) {
                        tuple.setStartTimestamp(insertedTuple.getStartTimestamp());
                    }
                }
            }
            currentNode = parentNode;
            parentNode = parentNode.getParentNode();
        }
    }

    /*private void updateMetaInInternalNodeOpt(InternalNodeTuple insertedTuple) {
        TreeNode currentNode = this;
        TreeNode parentNode = currentNode.getParentNode();
        while (parentNode != null) {

            InternalNode internalNode = (InternalNode) parentNode;
            List<InternalNodeTuple> tuples = internalNode.getTuples();
            for (int i = tuples.size() - 1; i >=0; i--) {
                InternalNodeTuple tuple = tuples.get(i);
                //System.out.println(tuples.size() - i);
                if (tuple.getBlockId().equals(currentNode.getBlockId())) {
                    if (tuple.getStopTimestamp() < insertedTuple.getStopTimestamp()) {
                        tuple.setStopTimestamp(insertedTuple.getStopTimestamp());
                    }
                    if (tuple.getStartTimestamp() > insertedTuple.getStartTimestamp()) {
                        tuple.setStartTimestamp(insertedTuple.getStartTimestamp());
                    }
                }
            }
            currentNode = parentNode;
            parentNode = parentNode.getParentNode();
        }
    }*/

    private static NodeTuple generateInternalNodeTuple(InternalNode internalNode) {
        InternalNodeTuple tuple = new InternalNodeTuple(internalNode.getBlockId(), getStartTimestamp(internalNode), getStopTimestamp(internalNode), internalNode);
        tuple.setNodeType(NodeType.INTERNAL);
        return tuple;
    }

    private static long getStartTimestamp(InternalNode node) {
        return node.getTuples().get(0).getStartTimestamp();
    }

    private static long getStopTimestamp(InternalNode node) {
        return node.getTuples().get(node.getTuples().size()-1).getStopTimestamp();
    }

    public List<InternalNodeTuple> getTuples() {
        return tuples;
    }

    public void setTuples(List<InternalNodeTuple> tuples) {
        this.tuples = tuples;
    }

    @Override
    void print() {
        System.out.println(toString());
    }

    @Override
    public String toString() {
        return "(" +
                "blockId=" + super.getBlockId() +
                ",\n" + tuples +
                ") ";
    }
}
