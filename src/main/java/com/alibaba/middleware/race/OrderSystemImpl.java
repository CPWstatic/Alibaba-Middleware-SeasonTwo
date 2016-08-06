package com.alibaba.middleware.race;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.race.engine.Database;
import com.alibaba.middleware.race.engine.Index;
import com.alibaba.middleware.race.engine.QueryMultiRowTask;
import com.alibaba.middleware.race.engine.QueryMultiRowWhereTask;
import com.alibaba.middleware.race.engine.QuerySingleRowTask;
import com.alibaba.middleware.race.engine.Row;
import com.alibaba.middleware.race.engine.Table;
import com.alibaba.middleware.race.engine.WhereStrategy;
import com.alibaba.middleware.race.util.Constructor;
import com.alibaba.middleware.race.util.CustomExecutorCompletionService;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.Weighers;

/**
 * 
 * @author root
 *
 */
public class OrderSystemImpl implements OrderSystem{
	
	private Database db;
	
	
//	private ExecutorService queryExecutor = Executors.newFixedThreadPool(16);
	
	private ExecutorService disk1Executor = Executors.newSingleThreadExecutor();
	
	private ExecutorService disk2Executor = Executors.newSingleThreadExecutor();
	
	private ExecutorService disk3Executor = Executors.newSingleThreadExecutor();

	public OrderSystemImpl(){
		
	}
	
	@Override
	public void construct(Collection<String> orderFiles, Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException, InterruptedException {
		long constructTime = System.currentTimeMillis();
		Constructor constructor = new Constructor();
		
		try {
			constructor.prepare(orderFiles, buyerFiles, goodFiles, storeFolders);
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		db = constructor.getDB();

		System.out.println("total Construct Time" + (System.currentTimeMillis() - constructTime));
		System.out.println(orderFiles);
		System.out.println(buyerFiles);
		System.out.println(goodFiles);
		System.out.println(storeFolders);

	}


	@Override
	public Result queryOrder(long orderId, Collection<String> keys) {
		long createTime = System.currentTimeMillis();
		
		ResultImp result = null;
		Row orderRow = null;
		Row buyerRow = null;
		Row goodRow = null;
		
		//查询订单
		orderRow = queryOrder(orderId);
		
		if(orderRow == null){
			//如果该订单不存在，返回null
			return null;
		}
		
		if(keys !=null && keys.size() == 0){
			//如果为空，则排除所有字段
			System.out.println(Thread.currentThread().getName() + " queryOrder: " + (System.currentTimeMillis()-createTime));
			return ResultImp.createNoRow(orderId);
		}
		
		//待查询的字段，如果为null，则查询所有字段
		if(keys == null){
			buyerRow = this.queryBuyer(orderRow.get("buyerid").valueAsString());
			goodRow = this.queryGood(orderRow.get("goodid").valueAsString());
			
			return ResultImp.createResultRow(orderRow, buyerRow, goodRow);
		}
		
		//join查询
		boolean joinBuyer = false;
		boolean joinGood = false;
		for(String key : keys){
			if(!db.getOrderColomns().contains(key)){
				if(db.getBuyerColomns().contains(key)){
					joinBuyer = true;
				}else if(db.getGoodColomns().contains(key)){
					joinGood = true;
				}
			}
		}
		
		//如果没有join，这里用keys过滤，可以保证如果不存在该字段，则排除所有字段
		if(!(joinBuyer || joinGood)){
			return ResultImp.createResultRow(orderRow, null, null, keys);
		}
		//join了buyer
		if(joinBuyer && !joinGood){
			buyerRow = this.queryBuyer(orderRow.get("buyerid").valueAsString());
			return ResultImp.createResultRow(orderRow, buyerRow, null, keys);
		}
		//join了good
		if(!joinBuyer && joinGood){
			goodRow = this.queryGood(orderRow.get("goodid").valueAsString());
			return ResultImp.createResultRow(orderRow, null, goodRow, keys);
		}
		System.out.println(Thread.currentThread().getName() + " queryOrder: " + (System.currentTimeMillis()-createTime));
		//join两个
		if(joinBuyer && joinGood){
			buyerRow = this.queryBuyer(orderRow.get("buyerid").valueAsString());
			goodRow = this.queryGood(orderRow.get("goodid").valueAsString());

			return ResultImp.createResultRow(orderRow, buyerRow, goodRow, keys);
		}
		
		//以上均不成里，不应出现
		return null;
	}

	
	class WhereStrategyImp implements WhereStrategy{
		private long startTime;
		private long endTime;
		private WhereStrategyImp(long startTime,long endTime){
			this.startTime = startTime;
			this.endTime = endTime;
		}
		@Override
		public boolean inRange(Row row) {
			try{
					if(row != null){
						long createTime = row.get("createtime").valueAsLong();
						if( createTime >= startTime && createTime < endTime){
							return true;
						}
					}
			}catch(TypeException e){
				e.printStackTrace();
			}
			return false;
		}
		
	}
	
	@Override
	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
		long queryTime = System.currentTimeMillis();
		if(buyerid == null){
			return null;
		}

		ArrayList<Result> results = new ArrayList<Result>();
		
		//从orders中查出所有符合条件的数据
		ArrayList<Row> orderRows = ordersMultiRowWhereQuery(buyerid,startTime,endTime);
		
		if(orderRows.size() == 0){
			//如果该buyerid不在order中，返回空值迭代器
			//TODO
			return results.iterator();
		}
		
		//join操作,该查询必须join全部
		// 已知buyer，只用查询一次
		Row buyerRow = this.queryBuyer(buyerid);
		//默认join所有
		for(Row orderRow : orderRows){
			Row goodRow = this.queryGood(orderRow.get("goodid").valueAsString());
			//默认join所有
			results.add(ResultImp.createResultRow(orderRow, buyerRow, goodRow));
		}
		
		//按照createtime大到小排列
		sortOnTime(results);
		
		System.out.println(" queryOrderByBuyer use time: " + (System.currentTimeMillis()-queryTime));
		return results.iterator();
	}

	
	
	@Override
	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
		boolean joinBuyer = false;
		boolean joinGood = false;
		boolean isOrder = false;
		if(keys == null){
			joinBuyer = true;
			joinGood = true;
			isOrder = true;
		}else{
			for(String key : keys){
				if(!db.getOrderColomns().contains(key)){
					if(db.getBuyerColomns().contains(key)){
						joinBuyer = true;
					}else if(db.getGoodColomns().contains(key)){
						joinGood = true;
					}
				}else{
					isOrder = true;
				}
			}
		}
		
		

		ArrayList<Result> results = new ArrayList<Result>();
		
		//查询订单
		ArrayList<Row> orderRows = ordersMultiRowQuery(goodid);
        
		//如果为空，则排除所有字段||如果不存在这个字段，则排除所有字段
		if((keys !=null && keys.size() == 0) || !(isOrder || joinBuyer || joinGood)){
			for(Row orderRow : orderRows){
				try {
					results.add(ResultImp.createNoRow(orderRow.get("orderid").valueAsLong()));
				} catch (TypeException e) {
					
				}
			}
			sortOnOrderid(results);
			return results.iterator();
		}
		
		//要查询的key都在order中
		if(isOrder && !(joinBuyer || joinGood)){
			for(Row orderRow : orderRows){
				results.add(ResultImp.createResultRow(orderRow,null,null));
			}
			
			sortOnOrderid(results);
			return results.iterator();
		}else if(joinGood && !joinBuyer){
			//join了good表,没有buyer
			Row goodRow = this.queryGood(goodid);
			for(Row orderRow : orderRows){
				results.add(ResultImp.createResultRow(orderRow, null, goodRow, keys));
			}
			sortOnOrderid(results);
			return results.iterator();
		}else if(!joinGood && joinBuyer){
			//join了buyer表,没有good
			for(Row orderRow : orderRows){
				Row buyerRow = this.queryBuyer(orderRow.get("buyerid").valueAsString());
				results.add(ResultImp.createResultRow(orderRow, buyerRow, null, keys));		
			}
			sortOnOrderid(results);
			return results.iterator();
		}else if(joinGood && joinBuyer){
			//join两张表
			Row goodRow = this.queryGood(goodid);
			for(Row orderRow : orderRows){
				Row buyerRow = this.queryBuyer(orderRow.get("buyerid").valueAsString());
				if(keys == null){
					//待查询的字段，如果为null，则查询所有字段
					results.add(ResultImp.createResultRow(orderRow, buyerRow, goodRow));
				}else{
					results.add(ResultImp.createResultRow(orderRow, buyerRow, goodRow, keys));
				}
			}
			sortOnOrderid(results);
			return results.iterator();
		}else{
			//不应该返回null
			return null;
		}

		
	}
	
	
	@Override
	public KeyValue sumOrdersByGood(String goodid, String key) {
		long queryTime = System.currentTimeMillis();
		boolean joinBuyer = false;
		boolean joinGood = false;
		if(!db.getOrderColomns().contains(key)){
			if(db.getBuyerColomns().contains(key)){
				joinBuyer = true;
			}else if(db.getGoodColomns().contains(key)){
				joinGood = true;
			}else{
				// 如果查询订单中的所有商品均不包含该字段，则返回null
				System.out.println(" sumUpByGood use time: " + (System.currentTimeMillis()-queryTime));
				return null;
			}
		}

//		ArrayList<Result> results = new ArrayList<Result>();

		//查询订单
		ArrayList<Row> orderRows = ordersMultiRowQuery(goodid);
		
		//如果求和的key中包含非long/double类型字段，则返回null
		if(!(joinBuyer || joinGood)){
			String sum = sumUp(orderRows,key);
			System.out.println(" sumUpByGood use time: " + (System.currentTimeMillis()-queryTime));

			return sum == null ? null:new KV(goodid,sum);
			
		}else if(joinBuyer && !joinGood){
			ArrayList<Row> buyerRows = new ArrayList<Row>();
			for(Row orderRow : orderRows){
				Row buyerRow = this.queryBuyer(orderRow.get("buyerid").valueAsString());
				buyerRows.add(buyerRow);
			}
			String sum = sumUp(buyerRows,key);
			System.out.println(" sumUpByGood use time: " + (System.currentTimeMillis()-queryTime));

			return sum == null ? null:new KV(goodid,sum);
		}else if(joinGood && !joinBuyer){
			Row goodRow = this.queryGood(goodid);
			//如果查询订单中的所有商品均不包含该字段，则返回null
			if(goodRow == null && goodRow.get(key) == null){
				return null;
			}
			
			ArrayList<Row> goodRows = new ArrayList<Row>();
			for(int i = 0; i < orderRows.size(); i++){
				goodRows.add(goodRow);
			}
			String sum = sumUp(goodRows,key);
			System.out.println(" sumUpByGood use time: " + (System.currentTimeMillis()-queryTime));

			return sum == null ? null:new KV(goodid,sum);
		}
		System.out.println(" sumUpByGood use time: " + (System.currentTimeMillis()-queryTime));
		return null;
	}
	
	/**
	 * 求和
	 * @param rows
	 * @param key
	 * @return
	 */
	private String sumUp(ArrayList<Row> rows , String key){
		if(rows == null){
			System.out.println("rows null");
			return null;
		}
		Double dsum = 0.0;
		Long lsum = (long) 0;
		boolean isLong = false;
		boolean isDouble = false;
//		System.out.println(rows);

		for(Row row : rows){
			KV kv = row.get(key);
//			System.out.println(kv + "\n");
			if(kv != null){
				if(kv.valueAsString().contains(".")){
					try {
						dsum += kv.valueAsDouble();
					} catch (TypeException e) {
						return null;
					}
					isDouble = true;
				}else{
					try {
						lsum += kv.valueAsLong();
					} catch (TypeException e) {
						return null;
					}
					isLong = true;
				}
			}
		}
		
		if(isLong && isDouble){
			Double sum = lsum + dsum;
			return sum.toString();
		}else if(isLong && !isDouble){
			return lsum.toString();
		}else if(!isLong && isDouble){
			return dsum.toString();
		}
		//如果查询订单中的所有商品均不包含该字段，则返回null
		return null;
	}
	
//	private Row joinBuyerQuery(String colomnValue){
//		//未找到返回null
//		for(Entry<String,Table> entry : db.getBuyerTables().entrySet()){
//			Table table = entry.getValue();
//			ArrayList<Long> offsets = table.getAnIndex(table.getTableName() + "buyerid").get(colomnValue);
//			if(offsets != null && offsets.size() == 1){
//				File file = new File(table.getPath());
//				try {
//					return FileReader.fileRead(file, offsets.get(0));
//				} catch (IOException e) {
//					System.out.println(e.getMessage()); 
//				}
//			}
//		}
//
//		return null;
//	}
	
//	private Row joinGoodQuery(String colomnValue){
//		for(Entry<String,Table> entry : db.getGoodTables().entrySet()){
//			Table table = entry.getValue();
//			ArrayList<Long> offsets = table.getAnIndex(table.getTableName() + "goodid").get(colomnValue);
//			if(offsets != null && offsets.size() == 1){
//				File file = new File(table.getPath());
//				try {
//					return FileReader.fileRead(file, offsets.get(0));
//				} catch (IOException e) {
//					System.out.println(e.getMessage());
//				}
//			}
//		}
//		
//		return null;
//	}

	/**
	 * 订单缓存
	 */
	ConcurrentLinkedHashMap<Long, Row> orderMap = new
		    ConcurrentLinkedHashMap.Builder<Long,Row>()
		            .maximumWeightedCapacity(5000).
		            weigher(Weighers.singleton()).build();
		    
	private Row queryOrder(long orderid){
		Row orderRow = orderMap.get(orderid);
		//缓存不存在，则查询
		if(orderRow != null){
			return orderRow;
		}else{
			orderRow = singleRowQuery(db.getOrderTables().entrySet(),"orderid",orderid);
			if(orderRow != null){
				orderMap.put(orderid, orderRow);
			}
			
			return orderRow;
		}
		
	}
	
	/**
	 * 买家缓存
	 */
	ConcurrentLinkedHashMap<String, Row> buyerMap = new
		    ConcurrentLinkedHashMap.Builder<String,Row>()
		            .maximumWeightedCapacity(5000).
		            weigher(Weighers.singleton()).build();
	
	private Row queryBuyer(String buyerid){
		Row buyerRow = buyerMap.get(buyerid);
		//
		if(buyerRow != null){
			return buyerRow;
		}else{
			buyerRow = singleRowQuery(db.getBuyerTables().entrySet(),"buyerid",buyerid);
			if(buyerRow != null){
				buyerMap.put(buyerid, buyerRow);
			}
			
			return buyerRow;
		}
	}
	
	/**
	 * 商品缓存
	 */
	ConcurrentLinkedHashMap<String, Row> goodMap = new
		    ConcurrentLinkedHashMap.Builder<String,Row>()
		            .maximumWeightedCapacity(5000).
		            weigher(Weighers.singleton()).build();

	private Row queryGood(String goodid){
		Row goodRow = goodMap.get(goodid);
		if(goodRow != null){
			return goodRow;
		}else{
			goodRow = singleRowQuery(db.getGoodTables().entrySet(),"goodid",goodid);
			if(goodRow != null){
				goodMap.put(goodid, goodRow);
			}
			
			return goodRow;
		}
	}
	
	/**
	 * 单行查询
	 * @param tables
	 * @param tableSub
	 * @param column
	 * @return
	 */
	private Row singleRowQuery(Set<Entry<String, Table>> tables, String tableSub, Object column){
		CustomExecutorCompletionService<Row> ecs
        	= new CustomExecutorCompletionService<Row>(disk1Executor,disk2Executor,disk3Executor);
		List<Future<Row>> futures
			= new ArrayList<Future<Row>>();
		// 提交任务
		for(Entry<String,Table> entry : tables){
			Table table = entry.getValue();
			for(Entry<String,Index> index : table.getIndexes().entrySet()){
				if(index.getKey().startsWith(table.getTableName() + tableSub)){
					if(table.getPath().startsWith("/disk1")){
						if(table.getTableName().startsWith("order"))
							futures.add(ecs.submit(new QuerySingleRowTask(index.getValue(),table.getPath(),column,false),"disk1"));
						else
							futures.add(ecs.submit(new QuerySingleRowTask(index.getValue(),table.getPath(),column,true),"disk1"));
					}
						
					if(table.getPath().startsWith("/disk2")){
						if(table.getTableName().startsWith("order"))
							futures.add(ecs.submit(new QuerySingleRowTask(index.getValue(),table.getPath(),column,false),"disk2"));
						else
							futures.add(ecs.submit(new QuerySingleRowTask(index.getValue(),table.getPath(),column,true),"disk2"));
					}
					
					if(table.getPath().startsWith("/disk3")){
						if(table.getTableName().startsWith("order"))
							futures.add(ecs.submit(new QuerySingleRowTask(index.getValue(),table.getPath(),column,false),"disk3"));
						else
							futures.add(ecs.submit(new QuerySingleRowTask(index.getValue(),table.getPath(),column,true),"disk3"));
					}
				}
			}
		}
		//取结果
		Row result = null;
		int n = futures.size();
		try{
			for(int i = 0; i < n; ++i){
				try {
					Row r = ecs.take().get();
				if(r != null){
					result = r;
					break;
				}
				} catch (InterruptedException e) {
					//ignore
				} catch (ExecutionException e) {
					//ignore
				}
			}
		}finally{
			//取消其他任务
			for (Future<Row> f : futures)
				f.cancel(true);
		}

		return result;
	}
	
	/**
	 * 订单的多行查询，带条件的
	 * @param buyerid
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private ArrayList<Row> ordersMultiRowWhereQuery(String buyerid,long startTime,long endTime){
		ArrayList<Row> orderRows= new ArrayList<Row>();
		BlockingQueue<Future<ArrayList<Row>>> queue = new LinkedBlockingQueue<Future<ArrayList<Row>>>();

		for(Entry<String,Table> entry : db.getOrderTables().entrySet()){
			Table table = entry.getValue();
			for(Entry<String,Index> index : table.getIndexes().entrySet()){
				if(index.getKey().startsWith(table.getTableName() + "buyerid")){
					if(table.getPath().startsWith("/disk1"))
						queue.add(disk1Executor.submit(new QueryMultiRowWhereTask(index.getValue()
																		,table.getPath()
																		,buyerid
																		,new WhereStrategyImp(startTime,endTime))));
					if(table.getPath().startsWith("/disk2"))
						queue.add(disk2Executor.submit(new QueryMultiRowWhereTask(index.getValue()
																		,table.getPath()
																		,buyerid
																		,new WhereStrategyImp(startTime,endTime))));
					if(table.getPath().startsWith("/disk3"))
						queue.add(disk3Executor.submit(new QueryMultiRowWhereTask(index.getValue()
																		,table.getPath()
																		,buyerid
																		,new WhereStrategyImp(startTime,endTime))));
				}
			}
		}
		
		int queueSize = queue.size();  
        for(int i=0; i<queueSize; i++){  
        	try {
				orderRows.addAll(queue.take().get());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
        }
        
        return orderRows;
	}

	/**
	 * 时间排序
	 * @param results
	 */
	private void sortOnTime(ArrayList<Result> results){
		Collections.sort(results,new Comparator<Result>(){

			@Override
			public int compare(Result o1, Result o2) {
				Long c1=(long) -1,c2=(long) -1;
				try{
					c1 = o1.get("createtime").valueAsLong();
					c2 = o2.get("createtime").valueAsLong();
				}catch(Exception e){
					System.out.println(e.getMessage());
				}
				return  c1>c2  ? -1 : 1;
			}
			
		});
	}
	

	/**
	 * 没有条件的订单多行查询
	 * @param goodid
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private ArrayList<Row> ordersMultiRowQuery(String goodid){
		ArrayList<Row> orderRows = new ArrayList<Row>();
		BlockingQueue<Future<ArrayList<Row>>> queue = new LinkedBlockingQueue<Future<ArrayList<Row>>>();
		//按照goodid查询,这是必须执行的步骤
		for(Entry<String,Table> entry : db.getOrderTables().entrySet()){
			Table table = entry.getValue();
			for(Entry<String,Index> index : table.getIndexes().entrySet()){
				if(index.getKey().startsWith(table.getTableName() + "goodid")){
					if(table.getPath().startsWith("/disk1"))
						queue.add(disk1Executor.submit(new QueryMultiRowTask(index.getValue()
																	,table.getPath()
																	,goodid)));
					if(table.getPath().startsWith("/disk2"))
						queue.add(disk2Executor.submit(new QueryMultiRowTask(index.getValue()
																	,table.getPath()
																	,goodid)));
					if(table.getPath().startsWith("/disk3"))
						queue.add(disk3Executor.submit(new QueryMultiRowTask(index.getValue()
																	,table.getPath()
																	,goodid)));
				}
			}
		}
		
		int queueSize = queue.size();  
        for(int i=0; i<queueSize; i++){  
        	try {
				orderRows.addAll(queue.take().get());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
        }
        
        return orderRows;
	}
	
	/**
	 * 根据订单排序
	 * @param results
	 */
	private void sortOnOrderid(ArrayList<Result> results){
		//按照订单id从小至大排序
		Collections.sort(results,new Comparator<Result>(){

			@Override
			public int compare(Result o1, Result o2) {
				Long c1=(long) -1,c2=(long) -1;
				try{
					c1 = o1.orderId();
					c2 = o2.orderId();
				}catch(Exception e){
					
				}
				return  c1>c2  ? 1 : -1;
			}
			
		});
	}


	
}
