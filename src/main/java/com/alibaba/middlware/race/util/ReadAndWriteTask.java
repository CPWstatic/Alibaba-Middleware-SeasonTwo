package com.alibaba.middlware.race.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.race.engine.Database;
import com.alibaba.middleware.race.engine.Index;
import com.alibaba.middleware.race.engine.Table;

public class ReadAndWriteTask implements Runnable {

	private LinkedBlockingQueue<String> diskQueue;
	private CountDownLatch countDownLatch;
	private Collection<String> storeFolders;
	private String diskName;
	private String indexRootPath;
	private Database database;
	private LinkedBlockingQueue<Entry<Index, File>> saveOrderQueue;
	private LinkedBlockingQueue<Entry<Index, File>> saveBuyerQueue;
	private LinkedBlockingQueue<Entry<Index, File>> saveGoodQueue;
	private ConcurrentHashMap<String, Integer> orderKeys;
	private ConcurrentHashMap<String, Integer> buyerKeys;
	private ConcurrentHashMap<String, Integer> goodKeys;
	private CountDownLatch latch = new CountDownLatch(3);

	public ReadAndWriteTask(String diskName, LinkedBlockingQueue<String> diskQueue, Collection<String> storeFolders,
			ConcurrentHashMap<String, Integer> orderKeys, ConcurrentHashMap<String, Integer> buyerKeys,
			ConcurrentHashMap<String, Integer> goodKeys, CountDownLatch countDownLatch, Database database) {
		this.diskName = diskName;
		this.diskQueue = diskQueue;
		this.countDownLatch = countDownLatch;
		this.storeFolders = storeFolders;
		this.database = database;

		this.orderKeys = orderKeys;
		this.buyerKeys = buyerKeys;
		this.goodKeys = goodKeys;

		this.saveBuyerQueue = new LinkedBlockingQueue<Entry<Index, File>>();
		this.saveGoodQueue = new LinkedBlockingQueue<Entry<Index, File>>();
		this.saveOrderQueue = new LinkedBlockingQueue<Entry<Index, File>>();
	}

	public void getIndexPath() {
		Iterator<String> storeIter = storeFolders.iterator();
		while (storeIter.hasNext()) {
			String path = storeIter.next();
			//TODO LINUX改成1
			String name = path.split("/")[1]; 
			if (diskName.equals(name)) {
				indexRootPath = path;
				break;
			}
		}
	}

	public void startSaveThread() {
		
		SaveTask saveOrderTask = new SaveTask(saveOrderQueue,latch);
		SaveTask saveBuyerTask = new SaveTask(saveBuyerQueue,latch);
		SaveTask saveGoodTask = new SaveTask(saveGoodQueue,latch);

		Thread saveOrderThread = new Thread(saveOrderTask, "saveOrderThread");
		Thread saveBuyerThread = new Thread(saveBuyerTask, "saveBuyerThread");
		Thread saveGoodThread = new Thread(saveGoodTask, "saveGoodThread");

		saveOrderThread.start();
		saveBuyerThread.start();
		saveGoodThread.start();

	}

	@Override
	public void run() {
		getIndexPath();
		startSaveThread();
		try {
			while (true) {
				String tablePath = diskQueue.take();
				if (tablePath.equals("end")) {
					break;
				}
				String[] pathSplit = tablePath.split("/");
				String tableName = pathSplit[pathSplit.length - 1];
				File tableFile = new File(tablePath);

				if (tableName.charAt(0) == 'o') { // 代表这是一个订单信息
					Index[] orderIndexes = null;
					HashSet<String> orderKey = new HashSet<String>();
					orderIndexes = orderFileOrderOpr(tableFile, tableName, tablePath, indexRootPath, orderKeys,
							orderKey, new String[] { "orderid", "buyerid", "goodid" });
					Table orderTable = new Table(tableName, tablePath, orderKey);
					orderIndexSave(orderIndexes, orderTable);
					database.setAnOrderTable(tablePath, orderTable);
				} else if (tableName.charAt(0) == 'b') { // 代表一个买家信息
					HashSet<String> buyerKey = new HashSet<String>();
					Table buyerTable = buyerOrGoodTableCreate(tableFile, tableName, tablePath, indexRootPath, buyerKeys,
							buyerKey, "buyerid");
					database.setABuyerTable(tablePath, buyerTable);
				} else { // 代表一个商品信息
					HashSet<String> goodKey = new HashSet<String>();
					Table goodTable = buyerOrGoodTableCreate(tableFile, tableName, tablePath, indexRootPath, goodKeys,
							goodKey, "goodid");
					database.setAGoodTable(tablePath, goodTable);
				}
			}

		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				saveOrderQueue.put(new Entry<Index, File>(true));
				saveBuyerQueue.put(new Entry<Index, File>(true));
				saveGoodQueue.put(new Entry<Index, File>(true));
			} catch (InterruptedException e) {
				System.out.println(e.getMessage());
			}
		}
		try {
			latch.await();
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
		}
		countDownLatch.countDown();
	}

	private void orderIndexSave(Index[] orderIndexes, Table orderTable) throws InterruptedException {
		Index saveOrderIndex = orderIndexes[0];
		orderTable.setAnIndex(saveOrderIndex.getIndexName(), saveOrderIndex);
		File orderIndexFile = new File(indexRootPath + saveOrderIndex.getIndexName());
		if (!orderIndexFile.exists()) {
			orderIndexFile.mkdirs();
		}
		saveOrderQueue.put(new Entry<Index, File>(saveOrderIndex, orderIndexFile));

		Index saveBuyerIndex = orderIndexes[1];
		orderTable.setAnIndex(saveBuyerIndex.getIndexName(), saveBuyerIndex);
		File buyerIndexFile = new File(indexRootPath + saveBuyerIndex.getIndexName());
		if (!buyerIndexFile.exists()) {
			buyerIndexFile.mkdirs();
		}
		saveBuyerQueue.put(new Entry<Index, File>(saveBuyerIndex, buyerIndexFile));

		Index saveGoodIndex = orderIndexes[2];
		orderTable.setAnIndex(saveGoodIndex.getIndexName(), saveGoodIndex);
		File goodIndexFile = new File(indexRootPath + saveGoodIndex.getIndexName());
		if (!goodIndexFile.exists()) {
			goodIndexFile.mkdirs();
		}
		saveOrderQueue.put(new Entry<Index, File>(saveGoodIndex, goodIndexFile));
	}

	@SuppressWarnings("unchecked")
	private Index[] orderFileOrderOpr(File tableFile, String tableName, String tablePath, String indexRootPath,
			ConcurrentHashMap<String, Integer> orderKeys, HashSet<String> orderKey, String[] indexNames)
			throws InterruptedException {
		LineIterator line = null;
		Object[] index_values;

		@SuppressWarnings("rawtypes")
		LinkedBlockingQueue[] queues = new LinkedBlockingQueue[3];
		Index[] indexes = new Index[3];

		indexes[0] = new Index(tableName + indexNames[0], true, new File(indexRootPath), new Comparator<Long>() {
			@Override
			public int compare(Long o1, Long o2) {
				return o1.compareTo(o2);
			}
		});

		for (int i = 0; i < 2; i++) {
			indexes[i + 1] = new Index(tableName + indexNames[i + 1], false, new File(indexRootPath),
					new Comparator<String>() {
						@Override
						public int compare(String o1, String o2) {
							return o1.compareTo(o2);
						}
					});
		}
		Thread[] t = new Thread[3];

		try {
			line = FileUtils.lineIterator(tableFile, "utf-8");
			queues[0] = new LinkedBlockingQueue<Entry<Long, Long>>();
			IndexInsertTask orderTask = new IndexInsertTask(queues[0], indexes[0]);
			t[0] = new Thread(orderTask);
			t[0].start();

			for (int i = 1; i < 3; i++) {
				queues[i] = new LinkedBlockingQueue<Entry<String, Long>>();
				IndexInsertTask task = new IndexInsertTask(queues[i], indexes[i]);
				t[i] = new Thread(task);
				t[i].start();
			}

			long lastNumberSize = 0L;
			while (line.hasNext()) {
				String sentence = line.nextLine();
				index_values = orderSplitSentence(sentence, indexNames, orderKeys, orderKey);
				queues[0].put(new Entry<Long, Long>((Long) index_values[0], lastNumberSize));
				for (int i = 1; i < 3; i++) {
					queues[i].put(new Entry<String, Long>((String) index_values[i], lastNumberSize));
					// indexes[i].insert(index_values[i], lastNumberSize);
				}
				lastNumberSize += (sentence.getBytes().length + 1);
			}
			for (int i = 0; i < 3; i++) {
				queues[i].put(new Entry<Object, Long>(true));
			}
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} finally {
			LineIterator.closeQuietly(line);
			for (int i = 0; i < 3; i++) {
				t[i].join();
			}
		}

		return indexes;
	}

	private Object[] orderSplitSentence(String sentence, String[] indexNames,
			ConcurrentHashMap<String, Integer> orderKeys, HashSet<String> orderKey) {
		String[] key_values = sentence.split("\t");
		Object[] index_value = new Object[indexNames.length];
		for (String key_value : key_values) {
			String[] kv = key_value.split(":");
			String key = kv[0];

			if (index_value[0] == null && key.equals(indexNames[0])) {
				index_value[0] = Long.parseLong(kv[1]);
			} else if (index_value[1] == null && key.equals(indexNames[1])) {
				index_value[1] = kv[1];
			} else if (index_value[2] == null && key.equals(indexNames[2])) {
				index_value[2] = kv[1];
			}
			orderKeys.put(key, 1);
			orderKey.add(key);

		}
		return index_value;
	}

	private Table buyerOrGoodTableCreate(File tableFile, String tableName, String tablePath, String indexRootPath,
			ConcurrentHashMap<String, Integer> keys, HashSet<String> key, String indexName) {
		Index buyerOrGoodIndex = null;
		buyerOrGoodIndex = buyerOrGoodfileOpr(tableFile, tableName, indexRootPath, keys, key, indexName);
		File indexFile = new File(indexRootPath + buyerOrGoodIndex.getIndexName());
		if (!indexFile.exists()) {
			indexFile.mkdirs();
		}

		buyerOrGoodIndex.saveIndex(indexFile);
		Table buyerTable = new Table(tableName, tablePath, key);
		buyerTable.setAnIndex(buyerOrGoodIndex.getIndexName(), buyerOrGoodIndex);
		return buyerTable;

	}

	private Index buyerOrGoodfileOpr(File tableFile, String tableName, String indexRootPath,
			ConcurrentHashMap<String, Integer> keys, HashSet<String> key, String indexName) {
		LineIterator line = null;
		Index index = new Index(tableName + indexName, true, new File(indexRootPath), new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}

		});
		String index_value = null;
		try {
			line = FileUtils.lineIterator(tableFile, "utf-8");
			long lastNumberSize = 0L;
			while (line.hasNext()) {
				String sentence = line.nextLine();
				index_value = buyerOrGoodSplitSentence(sentence, indexName, keys, key);
				index.insert(index_value, lastNumberSize);
				lastNumberSize += (sentence.getBytes().length + 1);
			}
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} finally {
			LineIterator.closeQuietly(line);
		}
		return index;
	}

	private String buyerOrGoodSplitSentence(String sentence, String indexName,
			ConcurrentHashMap<String, Integer> buyerOrGoodKeys, HashSet<String> buyerOrGoodKey) {
		String[] key_values = sentence.split("\t");
		String index_value = null;
		for (String key_value : key_values) {
			String[] kv = key_value.split(":");
			String key = kv[0];
			if (index_value == null && key.equals(indexName)) {
				index_value = kv[1];
			}
			buyerOrGoodKeys.put(key, 1);
			buyerOrGoodKey.add(key);

		}
		return index_value;
	}

}


class IndexInsertTask implements Runnable {

	private Index index;
	private LinkedBlockingQueue<Entry<Object, Long>> queue;

	public IndexInsertTask(LinkedBlockingQueue<Entry<Object, Long>> linkedBlockingQueue, Index index) {
		this.index = index;
		this.queue = linkedBlockingQueue;
	}

	@Override
	public void run() {
		Entry<Object, Long> kv;
		while (true) {
			try {
				kv = queue.take();
				Object key = kv.key;
				Long value = kv.value;
				boolean isFinished = kv.isFinished;
				if (isFinished) {
					break;
				}
				index.insert(key, value);
			} catch (InterruptedException e) {
				System.out.println(e.getMessage());
			}
		}
	}

}

class SaveTask implements Runnable {

	private LinkedBlockingQueue<Entry<Index, File>> queue;
	private CountDownLatch latch;
	public SaveTask(LinkedBlockingQueue<Entry<Index, File>> queue,CountDownLatch latch) {
		this.queue = queue;
		this.latch = latch;
	}

	@Override
	public void run() {
		Entry<Index, File> kv;
		while (true) {
			try {
				kv = queue.take();
				boolean isFinished = kv.isFinished;
				if (isFinished) {
					break;
				}
				Index key = kv.key;
				File value = kv.value;
				key.saveIndex(value);
			} catch (InterruptedException e) {
				System.out.println(e.getMessage());
			}
		}
		latch.countDown();
	}

}

class Entry<K, V> {
	K key;
	V value;
	boolean isFinished;

	public Entry(K key, V value) {
		this.key = key;
		this.value = value;
	}

	public Entry(boolean isFinished) {
		this.isFinished = isFinished;
	}

}