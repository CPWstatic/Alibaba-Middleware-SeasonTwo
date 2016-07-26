package com.alibaba.middlware.race.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.engine.Database;

public class FileLoader {

	// static final Logger logger = LoggerFactory.getLogger(FileLoader.class);
	private static ConcurrentHashMap<String, Integer> orderKeys = new ConcurrentHashMap<String, Integer>();
	private static ConcurrentHashMap<String, Integer> buyerKeys = new ConcurrentHashMap<String, Integer>();
	private static ConcurrentHashMap<String, Integer> goodKeys = new ConcurrentHashMap<String, Integer>();
	private static Database database = Database.getInstance("orderSystem");

	public void prepare(Collection<String> orderFiles, Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException, InterruptedException, ExecutionException {
		long createTime = System.currentTimeMillis();

		Iterator<String> orderFilesIterator = orderFiles.iterator();
		Iterator<String> buyerFilesIterator = buyerFiles.iterator();
		Iterator<String> goodFilesIterator = goodFiles.iterator();

		LinkedBlockingQueue<String> disk1Queue = new LinkedBlockingQueue<>();
		LinkedBlockingQueue<String> disk2Queue = new LinkedBlockingQueue<>();
		LinkedBlockingQueue<String> disk3Queue = new LinkedBlockingQueue<>();

		CountDownLatch countDownLatch = new CountDownLatch(3);
		
		ReadAndWriteTask disk1Task = new ReadAndWriteTask("disk1", disk1Queue, storeFolders, orderKeys, buyerKeys,
				goodKeys, countDownLatch, database);
		ReadAndWriteTask disk2Task = new ReadAndWriteTask("disk2", disk2Queue, storeFolders, orderKeys, buyerKeys,
				goodKeys, countDownLatch, database);
		ReadAndWriteTask disk3Task = new ReadAndWriteTask("disk3", disk3Queue, storeFolders, orderKeys, buyerKeys,
				goodKeys, countDownLatch, database);

		Thread disk1Thread = new Thread(disk1Task, "disk1Thread");
		Thread disk2Thread = new Thread(disk2Task, "disk2Thread");
		Thread disk3Thread = new Thread(disk3Task, "disk3Thread");

		disk1Thread.start();
		disk2Thread.start();
		disk3Thread.start();

		collectorTraversal(orderFilesIterator, disk1Queue, disk2Queue, disk3Queue);
		collectorTraversal(buyerFilesIterator, disk1Queue, disk2Queue, disk3Queue);
		collectorTraversal(goodFilesIterator, disk1Queue, disk2Queue, disk3Queue);
		disk1Queue.add("end");
		disk2Queue.add("end");
		disk3Queue.add("end");
		countDownLatch.await();

		System.out.println("total time:" + (System.currentTimeMillis() - createTime));
		System.out.println("orderKeySize:" + orderKeys.size());
		System.out.println("goodKeySize:" + goodKeys.size());
		System.out.println("buyerKeySize:" + buyerKeys.size());
		System.out.println("total time:" + (System.currentTimeMillis() - createTime));
	}

	private void collectorTraversal(Iterator<String> fileIterator, LinkedBlockingQueue<String> disk1Queue,
			LinkedBlockingQueue<String> disk2Queue, LinkedBlockingQueue<String> disk3Queue)
			throws InterruptedException {
		while (fileIterator.hasNext()) {
			String tableName = fileIterator.next();
			
			String disk = tableName.split("/")[1]; 
			if (disk.equals("disk1")) { 
				disk1Queue.put(tableName);
			} else if (disk.equals("disk2")) {
				disk2Queue.put(tableName);
			} else if (disk.equals("disk3")) {
				disk3Queue.put(tableName);
			}
		}
	}

	public Database getDB() {
		database.setOrderColomns(new ArrayList<String>(orderKeys.keySet()));
		database.setGoodColomns(new ArrayList<String>(goodKeys.keySet()));
		database.setBuyerColomns(new ArrayList<String>(buyerKeys.keySet()));
		return database;
	}

	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		String storePath1 = "/disk1/stores/";
		String storePath2 = "/disk2/stores/";
		String storePath3 = "/disk3/stores/";

		Set<String> storeSet = new HashSet<String>();
		Set<String> orderSet = new HashSet<String>();
		Set<String> buyerSet = new HashSet<String>();
		Set<String> goodSet = new HashSet<String>();

		storeSet.add(storePath1);
		storeSet.add(storePath2);
		storeSet.add(storePath3);

		orderSet.add("/disk1/orders/order.0.3");
		orderSet.add("/disk1/orders/order.0.0");
		orderSet.add("/disk2/orders/order.1.1");
		orderSet.add("/disk3/orders/order.2.2");

		buyerSet.add("/disk1/buyers/buyer.0.0");
		buyerSet.add("/disk2/buyers/buyer.1.1");

		goodSet.add("/disk1/goods/good.0.0");
		goodSet.add("/disk2/goods/good.1.1");
		goodSet.add("/disk3/goods/good.2.2");

		FileLoader fileReader = new FileLoader();

		System.out.println("singleThread: ");

		fileReader.prepare(orderSet, buyerSet, goodSet, storeSet);

	}
}



