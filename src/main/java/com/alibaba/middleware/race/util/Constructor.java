package com.alibaba.middleware.race.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.alibaba.middleware.race.engine.Database;
import com.alibaba.middleware.race.engine.Index;
import com.alibaba.middleware.race.engine.Table;

public class Constructor {

	private static ConcurrentHashMap<String, Integer> orderKeys = new ConcurrentHashMap<String, Integer>();
	private static ConcurrentHashMap<String, Integer> buyerKeys = new ConcurrentHashMap<String, Integer>();
	private static ConcurrentHashMap<String, Integer> goodKeys = new ConcurrentHashMap<String, Integer>();
	private static Database database = Database.getInstance("orderSystem");
	
    private final ThreadPoolExecutor disk1Exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    private final ThreadPoolExecutor disk2Exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    private final ThreadPoolExecutor disk3Exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    private ArrayList<Future<Object>> saveFutures = new ArrayList<Future<Object>>();
    private ArrayList<String> storeFolders;
    
	private static final int orderDivideSize = 10000000;
	private static final int buyerOrGoodDivideSize = 10000000;
	
	public void prepare(Collection<String> orderFiles, Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException, InterruptedException, ExecutionException {

		long createTime = System.currentTimeMillis();
		ArrayList<String> disk1Files = new ArrayList<>();
		ArrayList<String> disk2Files = new ArrayList<>();
		ArrayList<String> disk3Files = new ArrayList<>();
		this.storeFolders = new ArrayList<String>(storeFolders);
		
		for (String orderFileName : orderFiles) {
			if (orderFileName.charAt(5) == '1') {
				disk1Files.add(orderFileName);
			} else if (orderFileName.charAt(5) == '2') {
				disk2Files.add(orderFileName);
			} else {
				disk3Files.add(orderFileName);
			}
		}

		for (String buyerFileName : buyerFiles) {
			if (buyerFileName.charAt(5) == '1') {
				disk1Files.add(buyerFileName);
			} else if (buyerFileName.charAt(5) == '2') {
				disk2Files.add(buyerFileName);
			} else {
				disk3Files.add(buyerFileName);
			}
		}

		for (String goodFileName : goodFiles) {
			if (goodFileName.charAt(5) == '1') {
				disk1Files.add(goodFileName);
			} else if (goodFileName.charAt(5) == '2') {
				disk2Files.add(goodFileName);
			} else {
				disk3Files.add(goodFileName);
			}
		}
		
		for (String file : disk1Files) {
			operate(file);
		}

		for (String file : disk2Files) {
			operate(file);
		}

		for (String file : disk3Files) {
			operate(file);
		}
		System.out.println("disk1 complete" + disk1Exe.getCompletedTaskCount() + " disk2 complete" + disk2Exe.getCompletedTaskCount()+ " disk3 complete" +disk3Exe.getCompletedTaskCount());
	}
	
	private void operate(String tablePath) {
		int index = tablePath.lastIndexOf("/");
		String tableName = tablePath.substring(index + 1, tablePath.length());
		Table table = new Table(tableName, tablePath, null);

		if (tableName.charAt(0) == 'o') { // order file
			HashSet<String> orderKey = new HashSet<String>();
			orderFileOrderOpr(tablePath,new String[]{"orderid","buyerid","goodid"},table,orderKey);
			table.setColomns(orderKey);
			database.setAnOrderTable(tablePath, table);
		} else if (tableName.charAt(0) == 'b') { // buyer file
			HashSet<String> buyerKey = new HashSet<String>();
			buyerOrGoodFileOrderOpr(tablePath,"buyerid",table,buyerKey,true);	
			table.setColomns(buyerKey);
			database.setABuyerTable(tablePath, table);
		} else { // good file
			HashSet<String> goodKey = new HashSet<String>();
			buyerOrGoodFileOrderOpr(tablePath,"goodid",table,goodKey,false);	
			table.setColomns(goodKey);
			database.setAGoodTable(tablePath, table);
		}

	}
	
	private void orderFileOrderOpr(String tablePath, String[] indexNames,
			Table table, HashSet<String> orderKey){
		LineIterator line = null;
		Object[] index_values;
		File tableFile = new File(tablePath);
		try {
			line = FileUtils.lineIterator(tableFile, "utf-8");
			long lastNumberSize = 0L;
			int line_num = 0;
			int index_num = 0;
			
			//创建index，及执行构建线程
			Index[] indexes = createMultiIndex(table.getTableName(), indexNames, null,index_num);
			ArrayList<Future<Long>> futures = new ArrayList<Future<Long>>(3);
			IndexCreater[] indexCreaters = new IndexCreater[]{new IndexCreater(indexes[0],disk1Exe,disk2Exe,disk3Exe,table,saveFutures,storeFolders)
																,new IndexCreater(indexes[1],disk1Exe,disk2Exe,disk3Exe,table,saveFutures,storeFolders)
																,new IndexCreater(indexes[2],disk1Exe,disk2Exe,disk3Exe,table,saveFutures,storeFolders)};
			for(IndexCreater indexc :indexCreaters){
				futures.add(indexc.startup());
			}
			
			while (line.hasNext()) {
				String sentence = line.nextLine();
				index_values = orderSplitSentence(sentence, indexNames, orderKey);
				//提交任务
				for (int i = 0; i < 3; i++) {
					indexCreaters[i].put(new Object[]{index_values[i], lastNumberSize});
				}
				//超量处理
				if ((line_num != 0) && (line_num ^ orderDivideSize) == 0) {
//					orderIndexSave(indexes, indexRootPath, table);
					for(IndexCreater indexc : indexCreaters){
						indexc.shutdown();
					}
					//等待index建立完成
					for(Future<Long> future : futures){
						try {
							future.get();
						} catch (ExecutionException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
					line_num = 0;
					index_num++;
					indexes = createMultiIndex(table.getTableName(), indexNames, null,index_num);
					futures = new ArrayList<Future<Long>>(3);
					indexCreaters = new IndexCreater[]{new IndexCreater(indexes[0],disk1Exe,disk2Exe,disk3Exe,table,saveFutures,storeFolders)
																		,new IndexCreater(indexes[1],disk1Exe,disk2Exe,disk3Exe,table,saveFutures,storeFolders)
																		,new IndexCreater(indexes[2],disk1Exe,disk2Exe,disk3Exe,table,saveFutures,storeFolders)};
					for(IndexCreater indexc :indexCreaters){
						futures.add(indexc.startup());
					}
				}
				lastNumberSize += (sentence.getBytes().length + 1);
				line_num++;
			}
//			orderIndexSave(indexes, indexRootPath, table);
			for(IndexCreater indexc : indexCreaters){
				indexc.shutdown();
			}
			//等待index建立完成
			for(Future<Long> future : futures){
				try {
					future.get();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			indexes = null;
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch(InterruptedException ie){
			System.out.println(ie.getMessage());
		}finally {
			LineIterator.closeQuietly(line);
		}
	}
	
	private Object[] orderSplitSentence(String sentence, String[] indexNames, HashSet<String> orderKey) {
		String[] key_values = sentence.split("\t");
		Object[] index_value = new Object[indexNames.length];
		for (String key_value : key_values) {
			int index = key_value.indexOf(':');
			String key = key_value.substring(0, index);
			String value = key_value.substring(index + 1, key_value.length());
			if (index_value[0] == null && key.equals(indexNames[0])) {
				index_value[0] = Long.parseLong(value);
			} else if (index_value[1] == null && key.equals(indexNames[1])) {
				index_value[1] = value;
			} else if (index_value[2] == null && key.equals(indexNames[2])) {
				index_value[2] = value;
			}
			orderKeys.put(key, 1);
			orderKey.add(key);
		}
		return index_value;
	}
	
	private Index[] createMultiIndex(String tableName, String[] indexNames, String indexRootPath,int index_num) {
		Index[] indexes = new Index[3];

		indexes[0] = new Index(tableName + indexNames[0] + index_num, true, null, Long.class);
		for (int i = 0; i < 2; i++) {
			indexes[i + 1] = new Index(tableName + indexNames[i + 1] + index_num, false, null,
					String.class);
		}
		return indexes;

	}

	private Index createSingleIndex(String tableName, String indexName, String indexRootPath,int index_num) {
		Index index = new Index(tableName + indexName + index_num, true, null, String.class);
		return index;
	}
	
	private void buyerOrGoodFileOrderOpr(String tablePath, String indexName,
			Table table, HashSet<String> buyerOrGoodKey,boolean flag){
		LineIterator line = null;
		String index_value = null;
		File tableFile = new File(tablePath);
		try {
			line = FileUtils.lineIterator(tableFile, "utf-8");
			long lastNumberSize = 0L;
			int line_num = 0;
			int index_num = 0;

			Index buyerOrGoodIndex = createSingleIndex(table.getTableName(), indexName, null, index_num);
			IndexCreater indexc = new IndexCreater(buyerOrGoodIndex,disk1Exe,disk2Exe,disk3Exe,table,saveFutures,storeFolders);
			Future future = indexc.startup();
			while (line.hasNext()) {
				String sentence = line.nextLine();
				index_value = buyerOrGoodSplitSentence(sentence, indexName, buyerOrGoodKey,flag);
				indexc.put(new Object[]{index_value, lastNumberSize});
				if ((line_num != 0) && (line_num ^ buyerOrGoodDivideSize) == 0) {
					indexc.shutdown();
					try {
						future.get();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
					
					line_num = 0;
					index_num++;
					buyerOrGoodIndex = createSingleIndex(table.getTableName(), indexName, null,index_num);
					indexc = new IndexCreater(buyerOrGoodIndex,disk1Exe,disk2Exe,disk3Exe,table,saveFutures,storeFolders);
					future = indexc.startup();
				}
				lastNumberSize += (sentence.getBytes().length + 1);
				line_num++;
			}
			
			indexc.shutdown();
			try {
				future.get();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 


		} catch (IOException e) {
			System.out.println(e.getMessage());
		}finally {
			LineIterator.closeQuietly(line);
		}
	}

	private String buyerOrGoodSplitSentence(String sentence, String indexName, HashSet<String> buyerOrGoodKey,boolean flag) {
		String[] key_values = sentence.split("\t");
		String index_value = null;
		for (String key_value : key_values) {
			int index = key_value.indexOf(':');
			String key = key_value.substring(0,index);
			String value = key_value.substring(index+1,key_value.length());
			if (index_value == null && key.equals(indexName)) {
				index_value = value;
			}
			if(flag){
				buyerKeys.put(key, 1);				
			}else{
				goodKeys.put(key, 1);
			}
			buyerOrGoodKey.add(key);

		}
		return index_value;
	}
	
//	private void orderIndexSave(Index[] orderIndexes, String indexRootPath, Table orderTable)
//			throws InterruptedException {
//
//		Index saveOrderIndex = orderIndexes[0];
//		orderTable.setAnIndex(saveOrderIndex.getIndexName(), saveOrderIndex);
//		File orderIndexFile = new File(indexRootPath + saveOrderIndex.getIndexName());
//		if (!orderIndexFile.exists()) {
//			orderIndexFile.mkdirs();
//		}
//		saveOrderQueue.put(new Entry<Index, File>(saveOrderIndex, orderIndexFile));
//
//		Index saveBuyerIndex = orderIndexes[1];
//		orderTable.setAnIndex(saveBuyerIndex.getIndexName(), saveBuyerIndex);
//		File buyerIndexFile = new File(indexRootPath + saveBuyerIndex.getIndexName());
//		if (!buyerIndexFile.exists()) {
//			buyerIndexFile.mkdirs();
//		}
//		saveBuyerQueue.put(new Entry<Index, File>(saveBuyerIndex, buyerIndexFile));
//
//		Index saveGoodIndex = orderIndexes[2];
//		orderTable.setAnIndex(saveGoodIndex.getIndexName(), saveGoodIndex);
//		File goodIndexFile = new File(indexRootPath + saveGoodIndex.getIndexName());
//		if (!goodIndexFile.exists()) {
//			goodIndexFile.mkdirs();
//		}
//		saveGoodQueue.put(new Entry<Index, File>(saveGoodIndex, goodIndexFile));
//	}

	
	private void buyerOrGoodIndexSave(Index index, String indexRootPath, Table table) {
		table.setAnIndex(index.getIndexName(), index);
		File indexFile = new File(indexRootPath + index.getIndexName());
		if (!indexFile.exists()) {
			indexFile.mkdirs();
		}
		index.saveIndex(indexFile);
	}
	

	public Database getDB() {
		database.setOrderColomns(new ArrayList<String>(orderKeys.keySet()));
		database.setGoodColomns(new ArrayList<String>(goodKeys.keySet()));
		database.setBuyerColomns(new ArrayList<String>(buyerKeys.keySet()));
		for(Future future:saveFutures){
			try {
				future.get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		disk1Exe.shutdown();
		disk2Exe.shutdown();
		disk3Exe.shutdown();
		return database;
	}

}
