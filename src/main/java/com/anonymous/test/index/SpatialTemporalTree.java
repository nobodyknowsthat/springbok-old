package com.anonymous.test.index;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.anonymous.test.index.predicate.BasicQueryPredicate;
import com.anonymous.test.index.predicate.IdTemporalQueryPredicate;
import com.anonymous.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import com.anonymous.test.index.recovery.LeafNodeStatusRecorder;
import com.anonymous.test.index.spatial.TwoLevelGridIndex;
import com.anonymous.test.index.util.IndexConfiguration;
import com.anonymous.test.index.util.IndexSerializationUtil;
import com.anonymous.test.storage.driver.ObjectStoreDriver;
import com.anonymous.test.storage.driver.PersistenceDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import java.io.File;
import java.util.*;

/**
 * @Description
 * @Date 2021/3/16 14:31
 * @Created by X1 Carbon
 */
public class SpatialTemporalTree {

    @JsonIgnore
    private TreeNode root;

    @JsonIgnore
    private LeafNode activeNode;

    private int blockSize;

    private int nodeCount;  // used to generate block id for each node

    private int height;

    @JsonIgnore
    private LeafNodeStatusRecorder leafNodeStatusRecorder;

    @JsonIgnore
    int temporalEntryNum = 0;

    @JsonIgnore
    int spatialEntryNum = 0;

    @JsonIgnore
    private IndexConfiguration indexConfiguration = new IndexConfiguration();

    @JsonIgnore
    private TwoLevelGridIndex spatialTwoLevelGridIndex;

    private NodeType rootType;  // used for rebuild process

    private String rootNodeBlockId;  // used for rebuild process

    private PersistenceDriver persistenceDriver;

    private static Logger logger = LoggerFactory.getLogger(SpatialTemporalTree.class);

    @Deprecated
    public SpatialTemporalTree(int blockSize) {
        this.activeNode = new LeafNode(this);
        this.root = this.activeNode;
        this.blockSize = blockSize;
        this.nodeCount = 0;
        this.spatialTwoLevelGridIndex = new TwoLevelGridIndex();
        this.rootType = NodeType.LEAF;

        this.root.setBlockId(String.valueOf(generateBlockId()));
        ((LeafNode)this.root).getSpatialIndexNode().setBlockId(this.root.getBlockId());
        ((LeafNode)this.root).getTemporalIndexNode().setBlockId(this.root.getBlockId());
        this.root.setParentNode(null);
        this.root.setIndexTree(this);
        this.rootNodeBlockId = this.root.getBlockId();
    }

    public SpatialTemporalTree(IndexConfiguration indexConfiguration) {
        this.indexConfiguration = indexConfiguration;
        this.activeNode = new LeafNode(this);
        this.root = this.activeNode;
        this.blockSize = indexConfiguration.getBlockSize();
        this.nodeCount = 0;
        this.spatialTwoLevelGridIndex = new TwoLevelGridIndex();
        this.rootType = NodeType.LEAF;
        this.height = 1;

        this.root.setBlockId(String.valueOf(generateBlockId()));
        ((LeafNode)this.root).getSpatialIndexNode().setBlockId(this.root.getBlockId());
        ((LeafNode)this.root).getTemporalIndexNode().setBlockId(this.root.getBlockId());
        this.root.setParentNode(null);
        this.root.setIndexTree(this);
        this.rootNodeBlockId = this.root.getBlockId();

        this.persistenceDriver = new ObjectStoreDriver(indexConfiguration.getBucketNameInS3(), indexConfiguration.getRegionS3(), indexConfiguration.getRootDirnameInBucket());
    }

    /**
     * for in-memory test
     * @param indexConfiguration
     * @param index
     */
    @Deprecated
    public SpatialTemporalTree(IndexConfiguration indexConfiguration, TwoLevelGridIndex index) {
        this.indexConfiguration = indexConfiguration;
        this.activeNode = new LeafNode(this);
        this.root = this.activeNode;
        this.blockSize = indexConfiguration.getBlockSize();
        this.nodeCount = 0;
        this.spatialTwoLevelGridIndex = index;
        this.rootType = NodeType.LEAF;
        this.height = 1;

        this.root.setBlockId(String.valueOf(generateBlockId()));
        ((LeafNode)this.root).getSpatialIndexNode().setBlockId(this.root.getBlockId());
        ((LeafNode)this.root).getTemporalIndexNode().setBlockId(this.root.getBlockId());
        this.root.setParentNode(null);
        this.root.setIndexTree(this);
        this.rootNodeBlockId = this.root.getBlockId();
    }

    public static IndexConfiguration getDefaultIndexConfiguration() {
        Region region = Region.AP_EAST_1;
        String bucketName = "bucket-for-index-20101010";
        String rootDirnameInBucket = "index-test";
        int blockSize = 50;
        boolean isUseLazyParentUpdateForActiveNode = true;
        boolean isUsePreciseSpatialIndex = true;
        boolean isEnableSpatialIndex = true;
        IndexConfiguration indexConfiguration = new IndexConfiguration(blockSize, isUseLazyParentUpdateForActiveNode, bucketName, rootDirnameInBucket, region, isUsePreciseSpatialIndex, isEnableSpatialIndex);
        return indexConfiguration;
    }

    /**
     * only used for rebuild process
     */
    public SpatialTemporalTree() {}

    public String printStatus() {

        String status = "[SpatioTemporal Tree] status: nodeSize: " + blockSize + ", nodeCount: " + nodeCount + ", height: " + height + ", temporalEntryNum: " + temporalEntryNum + ", spatialEntryNum: " + spatialEntryNum;

        return status;
    }

    public void insert(TrajectorySegmentMeta meta) {

        if (indexConfiguration.isLazyParentUpdateForActiveNode()) {
            logger.info("insertion: using lazy mode to update parent nodes");
            activeNode.insertForLazyParentUpdate(meta);
        } else {
            logger.info("insertion: using common mode to update parent nodes");
            activeNode.insert(meta);
        }
    }

    @Deprecated
    public List<NodeTuple> search(BasicQueryPredicate predicate) {

        List<NodeTuple> resultTuples = new ArrayList<>();

        Queue<TreeNode> treeNodeQueue = new LinkedList<>();
        treeNodeQueue.add(root);

        while (!treeNodeQueue.isEmpty()) {
            TreeNode node = treeNodeQueue.poll();
            List<NodeTuple> results = node.search(predicate);
            for (NodeTuple nodeTuple : results) {
                if (nodeTuple instanceof InternalNodeTuple) {
                    InternalNodeTuple internalNodeTuple = (InternalNodeTuple) nodeTuple;
                    treeNodeQueue.offer(internalNodeTuple.getNodePointer());
                } else {
                    resultTuples.add(nodeTuple);
                }
            }

        }
        return removeDuplicateTuples(resultTuples);
    }

    public List<NodeTuple> searchForIdTemporal(IdTemporalQueryPredicate predicate) {
        List<NodeTuple> resultTuples = new ArrayList<>();

        Queue<TreeNode> treeNodeQueue = new LinkedList<>();
        if (indexConfiguration.isLazyParentUpdateForActiveNode() && predicate.getStopTimestamp() >= activeNode.getStartTimeOfFirstTuple()) {
            treeNodeQueue.add(activeNode);
        }
        if (!treeNodeQueue.contains(root)) {
            treeNodeQueue.add(root);
        }

        while (!treeNodeQueue.isEmpty()) {
            TreeNode node = treeNodeQueue.poll();
            List<NodeTuple> results = node.searchForIdTemporal(predicate);
            for (NodeTuple nodeTuple : results) {
                if (nodeTuple instanceof InternalNodeTuple) {
                    InternalNodeTuple internalNodeTuple = (InternalNodeTuple) nodeTuple;
                    treeNodeQueue.offer(internalNodeTuple.getNodePointer());
                } else {
                    resultTuples.add(nodeTuple);
                }
            }

        }
        return removeDuplicateTuples(resultTuples);
    }

    public List<NodeTuple> searchForSpatialTemporal(SpatialTemporalRangeQueryPredicate predicate) {
        List<NodeTuple> resultTuples = new ArrayList<>();

        Queue<TreeNode> treeNodeQueue = new LinkedList<>();
        if (indexConfiguration.isLazyParentUpdateForActiveNode() && predicate.getStopTimestamp() >= activeNode.getStartTimeOfFirstTuple()) {
            treeNodeQueue.add(activeNode);
        }
        treeNodeQueue.add(root);

        Set<String> appearedBlocks = new HashSet<>();
        while (!treeNodeQueue.isEmpty()) {
            TreeNode node = treeNodeQueue.poll();
            List<NodeTuple> results = node.searchForSpatialTemporal(predicate);
            for (NodeTuple nodeTuple : results) {
                if (nodeTuple instanceof InternalNodeTuple) {
                    InternalNodeTuple internalNodeTuple = (InternalNodeTuple) nodeTuple;
                    treeNodeQueue.offer(internalNodeTuple.getNodePointer());
                } else {
                    // some grids may point to the same data blocks
                    if (!appearedBlocks.contains(nodeTuple.getBlockId())) {
                        resultTuples.add(nodeTuple);
                        appearedBlocks.add(nodeTuple.getBlockId());
                    }
                }
            }

        }
        return removeDuplicateTuples(resultTuples);
    }

    private List<NodeTuple> removeDuplicateTuples(List<NodeTuple> tuples) {

        List<NodeTuple> resultTupleList = new ArrayList<>();
        Set<String> existedBlockSet = new HashSet<>();

        for (NodeTuple tuple : tuples) {
            if (!existedBlockSet.contains(tuple.getBlockId())) {
                resultTupleList.add(tuple);
                existedBlockSet.add(tuple.getBlockId());
            }
        }

        return resultTupleList;

    }

    public void close() {
        serializeAndFlushIndex();
    }

    /**
     * Serialize and flush the whole index tree, for ease to debug, each node will be stored as a seperate file
     */
    public void serializeAndFlushIndex() {
        SpatialTemporalTree tree = this;
        TreeNode rootNode = tree.getRoot();

        String filePathPrefix = persistenceDriver.getRootUri();

        // index tree meta persistence
        String indexTreeMetaString = IndexSerializationUtil.serializeIndexTreeMeta(tree);
        persistenceDriver.flush(filePathPrefix + File.separator + IndexSerializationUtil.TREE_META, indexTreeMetaString);


        if (rootNode instanceof LeafNode) {
            persistenceDriver.flush(filePathPrefix + File.separator + IndexSerializationUtil.ROOT_SPATIAL_FILENAME, IndexSerializationUtil.serializeLeafSpatialNode(((LeafNode) rootNode).getSpatialIndexNode()));
            persistenceDriver.flush(filePathPrefix + File.separator + IndexSerializationUtil.ROOT_TEMPORAL_FILENAME, IndexSerializationUtil.serializeLeafTemporalNode(((LeafNode) rootNode).getTemporalIndexNode()));

        }

        if (rootNode instanceof InternalNode) {
            InternalNode internalNode = (InternalNode) rootNode;
            persistenceDriver.flush(filePathPrefix + File.separator + IndexSerializationUtil.ROOT_INTERNAL_FILENAME, IndexSerializationUtil.serializeInternalNode(internalNode));

            for (InternalNodeTuple internalNodeTuple : internalNode.getTuples()) {
                traverseNodes(internalNodeTuple.getNodePointer());
            }
        }

    }

    private void traverseNodes(TreeNode treeNode) {
        String filePathPrefix = persistenceDriver.getRootUri();

        if (treeNode != null) {
            if (treeNode instanceof InternalNode) {
                InternalNode treeNodeInternal = (InternalNode) treeNode;
                String internalNodeString = IndexSerializationUtil.serializeInternalNode(treeNodeInternal);
                String filenameInternal = IndexSerializationUtil.generateInternalNodeFilename(treeNodeInternal.getBlockId());
                persistenceDriver.flush(filePathPrefix + File.separator + filenameInternal, internalNodeString);

                for (InternalNodeTuple internalNodeTuple : treeNodeInternal.getTuples()) {
                    traverseNodes(internalNodeTuple.getNodePointer());
                }
            }

            if (treeNode instanceof LeafNode) {

                LeafNode treeNodeLeaf = (LeafNode) treeNode;
                String temporalIndexString = IndexSerializationUtil.serializeLeafTemporalNode(treeNodeLeaf.getTemporalIndexNode());
                String filenameTemporal = IndexSerializationUtil.generateLeafTemporalNodeFilename(treeNodeLeaf.getBlockId());
                persistenceDriver.flush(filePathPrefix + File.separator + filenameTemporal, temporalIndexString);

                String spatialIndexString = IndexSerializationUtil.serializeLeafSpatialNode(treeNodeLeaf.getSpatialIndexNode());
                String filenameSpatial = IndexSerializationUtil.generateLeafSpatialNodeFilename(treeNodeLeaf.getBlockId());
                persistenceDriver.flush(filePathPrefix + File.separator + filenameSpatial, spatialIndexString);
            }
        }
    }

    /**
     * load the whole index tree from the file
     *  filename the filename of index tree meta
     * @return
     */
    public SpatialTemporalTree loadAndRebuildIndex() {

        String filename = IndexSerializationUtil.TREE_META;  // filename the filename of index tree meta

        String filePathPrefix = persistenceDriver.getRootUri();

        String indexTreeMetaFilename = filePathPrefix + File.separator + filename;
        String indexTreeMetaContent = persistenceDriver.getDataAsString(indexTreeMetaFilename);
        SpatialTemporalTree spatialTemporalTree = IndexSerializationUtil.deserializeIndexTreeMeta(indexTreeMetaContent);

        if (spatialTemporalTree.getRootType() == NodeType.LEAF) {
            // we only have one index node
            String leafTemporalNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.ROOT_TEMPORAL_FILENAME;
            String leafTemporalNodeContent = persistenceDriver.getDataAsString(leafTemporalNodeFilename);
            TemporalIndexNode temporalIndexNode = IndexSerializationUtil.deserializeLeafTemporalNode(leafTemporalNodeContent);
            String leafSpatialNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.ROOT_SPATIAL_FILENAME;
            String leafSpatialNodeContent = persistenceDriver.getDataAsString(leafSpatialNodeFilename);
            SpatialIndexNode spatialIndexNode = IndexSerializationUtil.deserializeLeafSpatialNode(leafSpatialNodeContent);

            LeafNode rootNode = new LeafNode();
            rootNode.setParentNode(null);
            rootNode.setIndexTree(spatialTemporalTree);
            rootNode.setBlockId(spatialTemporalTree.getRootNodeBlockId());
            rootNode.setTemporalIndexNode(temporalIndexNode);
            rootNode.setSpatialIndexNode(spatialIndexNode);

            temporalIndexNode.setParentNode(null);
            temporalIndexNode.setIndexTree(spatialTemporalTree);

            spatialIndexNode.setParentNode(null);
            spatialIndexNode.setIndexTree(spatialTemporalTree);

            spatialTemporalTree.setRoot(rootNode);
            // finally, we set the active node
            spatialTemporalTree.setActiveNode(rootNode);

        }

        if (spatialTemporalTree.getRootType() == NodeType.INTERNAL) {
            String rootInternalNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.ROOT_INTERNAL_FILENAME;
            String rootInternalNodeContent = persistenceDriver.getDataAsString(rootInternalNodeFilename);
            InternalNode rootNode = IndexSerializationUtil.deserializeInternalNode(rootInternalNodeContent);
            rootNode.setParentNode(null);
            rootNode.setIndexTree(spatialTemporalTree);
            rootNode.setBlockId(spatialTemporalTree.getRootNodeBlockId());

            spatialTemporalTree.setRoot(rootNode);

            for (InternalNodeTuple internalNodeTuple : rootNode.getTuples()) {
                traverseAndRebuildNodes(internalNodeTuple, rootNode, spatialTemporalTree);
            }

        }

        return spatialTemporalTree;
    }


    private void traverseAndRebuildNodes(NodeTuple nodeTuple, TreeNode parentNode, SpatialTemporalTree indexTree) {
        String filePathPrefix = persistenceDriver.getRootUri();

        if (nodeTuple.getNodeType() == NodeType.INTERNAL) {
            // rebuild the child node pointed by this tuple
            String nodeBlockId = nodeTuple.getBlockId();
            String internalNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.generateInternalNodeFilename(nodeBlockId);
            String internalNodeContent = persistenceDriver.getDataAsString(internalNodeFilename);
            InternalNode internalNode = IndexSerializationUtil.deserializeInternalNode(internalNodeContent);
            ((InternalNodeTuple) nodeTuple).setNodePointer(internalNode);
            internalNode.setParentNode(parentNode);
            internalNode.setIndexTree(indexTree);

            for (InternalNodeTuple internalNodeTuple : internalNode.getTuples()) {
                traverseAndRebuildNodes(internalNodeTuple, internalNode, indexTree);
            }
        }

        if (nodeTuple.getNodeType() == NodeType.LEAF) {
            String nodeBlockId = nodeTuple.getBlockId();
            String leafTemporalNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.generateLeafTemporalNodeFilename(nodeBlockId);
            String leafTemporalNodeContent = persistenceDriver.getDataAsString(leafTemporalNodeFilename);
            String leafSpatialNodeFilename = filePathPrefix + File.separator + IndexSerializationUtil.generateLeafSpatialNodeFilename(nodeBlockId);
            String leafSpatialNodeContent = persistenceDriver.getDataAsString(leafSpatialNodeFilename);

            TemporalIndexNode temporalIndexNode = IndexSerializationUtil.deserializeLeafTemporalNode(leafTemporalNodeContent);
            temporalIndexNode.setIndexTree(indexTree);
            temporalIndexNode.setParentNode(parentNode);
            SpatialIndexNode spatialIndexNode = IndexSerializationUtil.deserializeLeafSpatialNode(leafSpatialNodeContent);
            spatialIndexNode.setIndexTree(indexTree);
            spatialIndexNode.setParentNode(parentNode);

            LeafNode leafNode = new LeafNode();
            leafNode.setTemporalIndexNode(temporalIndexNode);
            leafNode.setSpatialIndexNode(spatialIndexNode);
            leafNode.setBlockId(nodeBlockId);
            leafNode.setIndexTree(indexTree);
            leafNode.setParentNode(parentNode);

            ((InternalNodeTuple)nodeTuple).setNodePointer(leafNode);

            // set active node (only last call needed)
            indexTree.setActiveNode(leafNode);

        }


    }

    public LeafNodeStatusRecorder getLeafNodeStatusRecorder() {
        return leafNodeStatusRecorder;
    }

    public void setLeafNodeStatusRecorder(LeafNodeStatusRecorder leafNodeStatusRecorder) {
        this.leafNodeStatusRecorder = leafNodeStatusRecorder;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int generateBlockId() {
        return nodeCount++;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public void setNodeCount(int nodeCount) {
        this.nodeCount = nodeCount;
    }

    public TreeNode getRoot() {
        return root;
    }

    public void setRoot(TreeNode root) {
        this.root = root;
    }

    public TreeNode getActiveNode() {
        return activeNode;
    }

    public void setActiveNode(LeafNode activeNode) {
        this.activeNode = activeNode;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setSpatialTwoLevelGridIndex(TwoLevelGridIndex spatialTwoLevelGridIndex) {
        this.spatialTwoLevelGridIndex = spatialTwoLevelGridIndex;
    }

    public NodeType getRootType() {
        return rootType;
    }

    public void setRootType(NodeType rootType) {
        this.rootType = rootType;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public String getRootNodeBlockId() {
        return rootNodeBlockId;
    }

    public void setRootNodeBlockId(String rootNodeBlockId) {
        this.rootNodeBlockId = rootNodeBlockId;
    }

    public void setIndexConfiguration(IndexConfiguration indexConfiguration) {
        this.indexConfiguration = indexConfiguration;
    }

    @Override
    public String toString() {
        return "nodeCount=" + (nodeCount+1) + "\n\nroot=" + root;

    }

    public TwoLevelGridIndex getSpatialTwoLevelGridIndex() {
        return spatialTwoLevelGridIndex;
    }

    public IndexConfiguration getIndexConfiguration() {
        return indexConfiguration;
    }
}
