package pw.hellojava.middlware.race.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 
 * @author root
 *
 */
public class BTree {
	// private static final Logger LOG = LoggerFactory.getLogger(BTree.class);

	/**
	 * degree b树的度 maxapacity b树单节点的key的数量至多= 2*degree-1
	 */
	private static final int DEFAULT_DEGREE = 1000;
	private int degree = DEFAULT_DEGREE;
	private int maxCapacity = 2 * degree - 1;

	/**
	 * keys 关键字 values 地址
	 */
	private ArrayList<Object> keys;
	private ArrayList<Long> values;

	/**
	 * childNodeIds 为子女们编号，作为指针，通过编号查找子女 loadedChildren 加载的子女们
	 */
	private ArrayList<Integer> childNodeIds;
	private Map<Integer, BTree> loadedChildren;

	/**
	 * indexName 当前b树节点所属的索引名 indexDir 当前索引的存放目录 fileId 当前节点点的Id（即地址）
	 */
	private String indexName;
	private File indexDir;
	private int currentNodeId;

	/**
	 * nodeIdCounter 当前索引的节点文件编号器，只有根节点有效
	 */
	private int nodeIdCounter;

	/**
	 * root 持有根节点的引用,根节点也会持有自己的引用
	 */
	private BTree root;

	/**
	 * comparator 关键字的比较器，策略模式
	 */
	private Comparator comparator;

	/**
	 * 创建一个root节点
	 * 
	 * @param indexName
	 * @param indexDir
	 * @param comparator
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public BTree(String indexName, File indexDir, Comparator comparator) throws ClassNotFoundException, IOException {
		this(DEFAULT_DEGREE, null, indexName, indexDir, comparator);
	}

	/**
	 * root == null表示创建新的root，！=null表示创建一个非root节点
	 * 
	 * @param degree
	 * @param root
	 * @param indexName
	 * @param indexDir
	 * @param comparator
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public BTree(int degree, BTree root, String indexName, File indexDir, Comparator comparator)
			throws IOException, ClassNotFoundException {
		this.degree = degree;
		this.maxCapacity = 2 * degree - 1;
		// keys至多2 * degree - 1
		this.keys = new ArrayList<Object>(maxCapacity);
		this.values = new ArrayList<Long>(maxCapacity);
		// 根和分支节点包含maxCapacity + 1个子女，叶子节点没有子女
		this.childNodeIds = new ArrayList<Integer>(maxCapacity + 1);
		// 用hashmap存储加载的子女
		loadedChildren = new HashMap<Integer, BTree>();
		//
		this.indexName = indexName;
		this.indexDir = indexDir;
		this.comparator = comparator;

		if (root != null) {
			this.root = root;
		} else {
			// 创建一个root
			this.root = this;

			// 如果存储索引的文件夹以及编号器文件存在，表明索引已经建立，需要将root节点的keys，values加载进来
			// 否则，仅仅设置id
			if (indexDir != null && this.getCounterFile().exists()) {
				// 因为是创建了root节点，所以root节点的id是0
				this.setNodeIdCounter(0);
				this.setCurrentNodeId(this.getNodeIdCounter());
				// 加载编号器
				loadCounterFile();
				// 读取root节点的数据
				readDisk();
			} else {
				allocate();
			}
		}
	}

	public BTree(int degree, BTree root, String indexName, File indexDir, Comparator comparator, boolean create)
			throws IOException, ClassNotFoundException {
		this(degree, root, indexName, indexDir, comparator);
		if (create) {
			allocate();
		}
	}

	/**
	 * Read in an existing node file
	 */
	public BTree(int nodeFileId, int degree, BTree root, String indexName, File indexDir, Comparator comparator)
			throws IOException, ClassNotFoundException {
		this(degree, root, indexName, indexDir, comparator);
		this.setCurrentNodeId(nodeFileId);
		readDisk();
	}

	/**
	 * 插入一个kv，当前所有为内存操作
	 * 
	 * @param key
	 * @param value
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public void insert(Object key, Long value) throws IOException, ClassNotFoundException {
		if (size() == maxCapacity) {
			// grow and rotate tree
			// if (LOG.isDebugEnabled()) {
			// LOG.debug(
			// "Node " + this.currentNodeId + " reached max capacity. Splitting
			// and rotating.");
			// }

			// LOG.debug("Creating new child node");
			BTree child = new BTree(degree, root, indexName, indexDir, comparator, true);

			// LOG.debug("Transferring all data to child");
			child.getKeys().addAll(keys);
			child.getValues().addAll(values);
			if (this.childNodeIds.size() > 0) {
				child.addChildrenFrom(this);
				// LOG.debug("Transferred children to child");
			}

			// LOG.debug("Emptying my data");
			keys.clear();
			values.clear();
			childNodeIds.clear();

			// LOG.debug("Attaching new child");
			addChild(0, child);

			// LOG.debug("Subdividing child into 2 children");
			subdivideChild(0, child);
		}
		insertNotfull(key, value);
	}

	/**
	 * 唯一索引可以使用该方法，找到最近的那个k，v
	 */
	public Long get(Object key) throws IOException, ClassNotFoundException {
		Long result = null;
		int i = findNearestKeyAbove(key);
		if ((i < size()) && (isEqual(key, getKeys().get(i)))) {
			result = getValues().get(i);
		} else if (!isLeaf()) {
			// recurse to children
			result = getChild(i).get(key);
		}
		// TODO
		// root节点应该释放内存
		this.release();

		return result;
	}

	/**
	 * 非唯一索引，使用getall，返回所有匹配
	 * 
	 * @param key
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public ArrayList<Long> getAll(Object key) throws IOException, ClassNotFoundException {
		ArrayList<Long> chain = new ArrayList<Long>();
		getAll(key, chain);
		// TODO
		// root节点应该释放内存
		this.release();

		return chain;
	}

	// ------------------------------------getters and
	// setters--------------------------------------------------

	private ArrayList<Object> getKeys() {
		return keys;
	}

	private ArrayList<Long> getValues() {
		return values;
	}

	private ArrayList<Integer> getChildNodeIds() {
		return childNodeIds;
	}

	private Map<Integer, BTree> getLoadedChildren() {
		return loadedChildren;
	}

	private int getCurrentNodeId() {
		return currentNodeId;
	}

	private void setCurrentNodeId(int nodeId) {
		this.currentNodeId = nodeId;
	}

	private int getNodeIdCounter() {
		if (isRoot()) {
			return this.nodeIdCounter;
		} else {
			return getRoot().getNodeIdCounter();
		}
	}

	private void setNodeIdCounter(int nodeIdCounter) {
		if (isRoot()) {
			this.nodeIdCounter = nodeIdCounter;
		} else {
			getRoot().setNodeIdCounter(nodeIdCounter);
		}
	}

	private BTree getRoot() {
		return root;
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * 只有root节点能够加载编号器 加载并设置root的编号器
	 */
	private void loadCounterFile() throws IOException {
		File idxCtr = getCounterFile();
		if (!isRoot()) {
			// If this isn't the root, then there is a problem.
			// LOG.warn("Non root attempting to load counter file");
			return;
		}
		FileInputStream fin = null;
		ObjectInputStream in = null;
		try {
			fin = new FileInputStream(idxCtr);
			in = new ObjectInputStream(fin);
			this.setNodeIdCounter(in.readInt());
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				in.close();
			} catch (Exception e) {
			}
			try {
				fin.close();
			} catch (Exception e) {
			}
		}
	}

	/**
	 * Saves out the counter file. This should only ever be called by the root.
	 */
	public void saveCounterFile() throws IOException {
		File idxCtr = getCounterFile();
		if (!isRoot()) {
			// If this isn't the root, then there is a problem.
			// LOG.warn("Non root attempting to save counter file");
			return;
		}
		FileOutputStream fout = null;
		ObjectOutputStream out = null;
		if (!idxCtr.exists()) {
			idxCtr.createNewFile();
		}
		// increment counter
		try {
			fout = new FileOutputStream(idxCtr);
			out = new ObjectOutputStream(fout);
			out.writeInt(getNodeIdCounter());
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				out.close();
			} catch (Exception e) {
			}
			try {
				fout.close();
			} catch (Exception e) {
			}
		}
	}

	private File getCounterFile() {
		return new File(indexDir, indexName + ".ctr");
	}

	/**
	 * //判断当前节点是否是root
	 * 
	 * @return
	 */
	private boolean isRoot() {
		return root == this;
	}

	private boolean isLeaf() {
		return (this.childNodeIds.size() == 0);
	}

	private int size() {
		return getKeys().size();
	}

	/*
	 * 
	 */
	private File getNodeFileById(int nodeId) {
		return new File(indexDir, indexName + "." + nodeId);
	}

	/**
	 * 设置节点ID
	 */
	private void allocate() {
		this.setCurrentNodeId(this.root.getNodeIdCounter());
		this.root.setNodeIdCounter(this.getCurrentNodeId() + 1);
	}

	// ---------------------------------------------------------------------------------------------------

	private void addChildrenFrom(BTree tree) {
		this.addChildren(tree.getChildNodeIds(), tree.getLoadedChildren());
	}

	private void addChildren(List<Integer> childNodeIds, Map<Integer, BTree> children) {
		for (Integer id : childNodeIds) {
			this.addChild(children.get(id));
		}
	}

	private void addChild(BTree child) {
		this.getChildNodeIds().add(child.getCurrentNodeId());
		this.loadedChildren.put(child.getCurrentNodeId(), child);
	}

	private void addChild(int index, BTree child) {
		this.getChildNodeIds().add(index, child.getCurrentNodeId());
		this.loadedChildren.put(child.getCurrentNodeId(), child);
	}

	private void subdivideChild(int pivot, BTree child) throws IOException, ClassNotFoundException {
		// if (LOG.isDebugEnabled()) {
		// LOG.debug("Parent keys: " + getKeys().toString());
		// LOG.debug("Child keys: " + child.getKeys().toString());
		// }
		int i = 0; // prepare index for loops below

		// LOG.debug("Create new child to take excess data");
		BTree fetus = new BTree(degree, root, indexName, indexDir, comparator, true);
		addChild(pivot + 1, fetus);

		// LOG.debug("Transfer (t-1) vals from existing child to new child");
		{
			List<Object> sub = child.getKeys().subList(degree, maxCapacity);
			// if (LOG.isDebugEnabled()) {
			// LOG.debug("Moving keys " + sub.toString());
			// }
			fetus.getKeys().addAll(sub);
		}
		List<Long> sub = child.getValues().subList(degree, maxCapacity);
		fetus.getValues().addAll(sub);
		if (!child.isLeaf()) {
			// LOG.debug("Transfer t children from existing child to new
			// child");
			List<Integer> ChildSub = child.getChildNodeIds().subList(degree, maxCapacity + 1);
			fetus.addChildren(ChildSub, child.getLoadedChildren());
			for (i = maxCapacity; i >= degree; i--) {
				child.getChildNodeIds().remove(i);
			}
		}

		// LOG.debug("Add new pivot key that divides children");
		Object pivotKey = child.getKeys().get(degree - 1);
		// if (LOG.isDebugEnabled()) {
		// LOG.debug("Pivot key: " + pivotKey);
		// }
		long pivotVal = child.getValues().get(degree - 1);
		getKeys().add(pivot, pivotKey);
		getValues().add(pivot, pivotVal);

		// LOG.debug("Trim child to new size");
		for (i = (maxCapacity - 1); i > (degree - 2); i--) {
			child.getKeys().remove(i);
			child.getValues().remove(i);
		}
	}

	private void insertNotfull(Object key, Long value) throws IOException, ClassNotFoundException {
		int i = findNearestKeyBelow(key);
		if (isLeaf()) {
			// if (LOG.isDebugEnabled()) {
			// LOG.debug("Node " + this.currentNodeId + " adding key " + key);
			// }
			getKeys().add(i + 1, key);
			getValues().add(i + 1, value);
		} else {
			// recurse
			// if (LOG.isDebugEnabled()) {
			// LOG.debug("Node " + this.currentNodeId + " is internal so adding
			// to child");
			// }
			i++;
			BTree child = getChild(i);
			if (child.size() == maxCapacity) {
				subdivideChild(i, child);
				if (isGreaterThan(key, getKeys().get(i))) {
					i++;
				}
			}
			getChild(i).insertNotfull(key, value);
		}
	}

	private BTree getChild(int index) throws IOException, ClassNotFoundException {
		if (index >= this.childNodeIds.size()) {
			throw new IOException("Node " + this.currentNodeId + " has no child at index " + index);
		} else {
			Integer nodeFileid = this.childNodeIds.get(index);
			if (loadedChildren.get(nodeFileid) == null) {
				loadedChildren.put(nodeFileid, new BTree(nodeFileid, degree, root, indexName, indexDir, comparator));
			}
			return loadedChildren.get(nodeFileid);
		}
	}

	private int findNearestKeyBelow(Object key) {
		int high = size();
		int low = 0;
		int cur = 0;

		// Short circuit
		if (size() == 0) {
			return -1;
		} else if (isLessThanOrEqual(getKeys().get(size() - 1), key)) {
			return size() - 1;
		} else if (isGreaterThan(getKeys().get(0), key)) {
			return -1;
		}

		while (low < high) {
			cur = (high + low) / 2;
			Object keyToComp = this.keys.get(cur);
			if (isEqual(key, keyToComp)) {
				// We found it now move to the last
				for (; (cur < size()) && isEqual(key, getKeys().get(cur)); cur++)
					;
				break;
			} else if (isGreaterThan(key, keyToComp)) {
				if (low == cur) {
					low++;
				} else {
					low = cur;
				}
			} else { // comp < 0
				high = cur;
			}
		}

		// Now go to the nearest if there are multiple entries
		for (; (cur >= 0) && (isLessThan(key, getKeys().get(cur))); cur--)
			;

		return cur;
	}

	private int findNearestKeyAbove(Object key) {
		int high = size();
		int low = 0;
		int cur = 0;

		// Short circuit
		if (size() == 0) {
			return 0;
		} else if (isLessThan(getKeys().get(size() - 1), key)) {
			return size();
		} else if (isGreaterThanOrEqual(getKeys().get(0), key)) {
			return 0;
		}

		while (low < high) {
			cur = (high + low) / 2;
			int comp = compare(key, getKeys().get(cur));
			if (high == low) {
				cur = low;
				break;
			} else if (comp == 0) {
				// We found it now move to the first
				for (; (cur > 0) && (isEqual(key, getKeys().get(cur))); cur--)
					;
				break;
			} else if (high - low == 1) {
				cur = high;
				break;
			} else if (comp > 0) {
				if (low == cur) {
					low++;
				} else {
					low = cur;
				}
			} else { // comp < 0
				high = cur;
			}
		}

		// Now go to the nearest if there are multiple entries
		for (; (cur < size()) && isGreaterThan(key, getKeys().get(cur)); cur++) {
		}
		;

		return cur;
	}

	private int compare(Object x, Object y) {
		return comparator.compare(x, y);
	}

	private boolean isEqual(Object x, Object y) {
		return compare(x, y) == 0;
	}

	private boolean isNotEqual(Object x, Object y) {
		return compare(x, y) != 0;
	}

	private boolean isGreaterThan(Object x, Object y) {
		return compare(x, y) > 0;
	}

	private boolean isGreaterThanOrEqual(Object x, Object y) {
		return compare(x, y) >= 0;
	}

	private boolean isLessThan(Object x, Object y) {
		return compare(x, y) < 0;
	}

	private boolean isLessThanOrEqual(Object x, Object y) {
		return compare(x, y) <= 0;
	}

	/**
	 * 
	 * 从最接近key的关键字开始，做中根遍历
	 * 
	 * @param key
	 * @param chain
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void getAll(Object key, ArrayList<Long> chain) throws IOException, ClassNotFoundException {
		int start = findNearestKeyAbove(key);
		if (isLeaf()) {
			int stop;
			for (stop = start; stop < size() && isEqual(key, getKeys().get(stop)); stop++) {
			}
			;
			chain.addAll(getValues().subList(start, stop));
		} else {
			int i = start;
			for (; i < size() && isEqual(key, getKeys().get(i)); i++) {
				getChild(i).getAll(key, chain);
				chain.add(getValues().get(i));
			}
			getChild(i).getAll(key, chain);
		}
	}

	/**
	 * root节点同时要保存编号器 递归，将整棵树写到磁盘，每个节点一个文件
	 */
	public void saveAll(File indexDirectory) throws IOException, ClassNotFoundException {
		indexDir = indexDirectory;
		saveAll();
		// root节点应该释放内存
		this.release();
	}

	private void saveAll() throws IOException, ClassNotFoundException {
		if (isRoot()) {
			saveCounterFile();
		}
		writeDisk();
		for (int i = 0; i < this.childNodeIds.size(); i++) {
			getChild(i).saveAll(indexDir);
		}
	}

	/**
	 * 将当前node写到磁盘 二进制流不方便debug 可以考虑json
	 */
	private void writeDisk() throws IOException {
		File nodeFile = getNodeFileById(this.currentNodeId);
		if (nodeFile == null) {
			throw new NullPointerException("ObjectBTree must be allocated before writing out to disk");
		}
		if (!nodeFile.exists()) {
			nodeFile.createNewFile();
		}
		// if (LOG.isDebugEnabled()) {
		// LOG.debug("Writing out file " + nodeFile);
		// LOG.debug(toString());
		// }
		FileOutputStream fout = new FileOutputStream(nodeFile);
		ObjectOutputStream out = new ObjectOutputStream(fout);
		out.writeInt(size());
		for (int i = 0; i < size(); i++) {
			out.writeObject(getKeys().get(i));
			out.writeLong(getValues().get(i));
		}
		out.writeInt(this.childNodeIds.size());
		for (int i = 0; i < this.childNodeIds.size(); i++) {
			out.writeInt(this.childNodeIds.get(i));
		}
		out.flush();
		out.close();
		fout.close();
	}

	/**
	 * 从文件读入当前节点的数据 二进制流不方便debug 可以考虑json
	 */
	private void readDisk() throws IOException, ClassNotFoundException {
		File nodeFile = getNodeFileById(this.currentNodeId);
		// if (LOG.isDebugEnabled()) {
		// LOG.debug("Reading in file " + nodeFile);
		// }
		FileInputStream fin = new FileInputStream(nodeFile);
		ObjectInputStream in = new ObjectInputStream(fin);
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			getKeys().add(in.readObject());
			getValues().add(in.readLong());
		}
		size = in.readInt();
		for (int i = 0; i < size; i++) {
			this.childNodeIds.add(in.readInt());
		}
		in.close();
		fin.close();
	}

	private void release() {
		if (this.isRoot()) {
			this.loadedChildren.clear();
		}
	}
}
