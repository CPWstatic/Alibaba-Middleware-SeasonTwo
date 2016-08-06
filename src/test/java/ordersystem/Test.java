package ordersystem;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.OrderSystem.TypeException;

public class Test {
	
	private static ExecutorService queryExecutor = Executors.newFixedThreadPool(4);
	
	private static OrderSystem orderSystem = new OrderSystemImpl();
	
	public static void main(String[] args) throws IOException, InterruptedException, TypeException {
		
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

		orderSystem.construct(orderSet, buyerSet, goodSet, storeSet);
		
		//QueryOrder
		Set<String> set = new LinkedHashSet<String>();
		set.add("contactphone");
		set.add("a_g_10209");
//		set.add("a_b_11255");
//		set.add("a_b_7337");
//		set.add("a_o_4699");
		
		long orderid = 612584687;
		QueryOrder q1 = new QueryOrder(orderid,set);
		
		//QueryOrdersByBuyer
		String buyerId = "wx-a0e0-6bda77db73ca";
		long startTime = 1462018520;
		long endTime = 1473999229;
		QueryOrdersByBuyer q2 = new QueryOrdersByBuyer(startTime, endTime, buyerId);
		
		//QueryOrdersBySaler
		String salerid = "almm-b250-b1880d628b9a";
		String goodid = "gd-b972-6926df8128c3";
		Collection<String> keys = new LinkedHashSet<String>();
		keys.add("a_o_30709");
		keys.add("a_g_32587");
		QueryOrdersBySaler q3 = new QueryOrdersBySaler(salerid,goodid,keys);
		
		//SumOrdersByGood
		String q4goodid = "al-96e5-7fac3721d4b9";
		String key = "amount";
		SumOrdersByGood q4 = new SumOrdersByGood(q4goodid,key);
		
		queryExecutor.submit(q1);
		queryExecutor.submit(q2);
		queryExecutor.submit(q3);
		queryExecutor.submit(q4);
//		Thread.sleep(10000);
//		queryExecutor.shutdown();
	}
	
	static class QueryOrder implements Runnable{

		private long orderId;
		private Collection<String> keys;
		public QueryOrder(long orderId, Collection<String> keys){
			this.orderId = orderId;
			this.keys = keys;
		}
		@Override
		public void run() {
			Result result = orderSystem.queryOrder(orderId, keys);
			KeyValue[] keyv = result.getAll();
			System.out.println("queryOrder: " + result.orderId());
			System.out.println(Arrays.toString(keyv));
		}	
		
	}
	
	static class QueryOrdersByBuyer implements Runnable{

		public QueryOrdersByBuyer(long startTime, long endTime, String buyerid) {
			super();
			this.startTime = startTime;
			this.endTime = endTime;
			this.buyerid = buyerid;
		}


		private long startTime;
		private long endTime;
		private String buyerid;
		
		
		@Override
		public void run() {
			System.out.println("queryOrderByBuyer");
			Iterator<Result> itr = orderSystem.queryOrdersByBuyer(startTime, endTime, buyerid);
			
			while(itr.hasNext()){
				Result result = itr.next();
				long orderId = result.orderId();
				KeyValue[] kv = result.getAll();
				System.out.print(orderId + " ");
				for(KeyValue keyvalue:kv){
					System.out.print(keyvalue);
				}
				System.out.println();
			}
		}
		
	}
	
	static class QueryOrdersBySaler implements Runnable{
		public QueryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
			super();
			this.salerid = salerid;
			this.goodid = goodid;
			this.keys = keys;
		}

		private String salerid;
		private String goodid;
		private Collection<String> keys;
		
		@Override
		public void run() {
			System.out.println("queryOrderBySaler");
			long start = System.currentTimeMillis();
			Iterator<Result> itr = orderSystem.queryOrdersBySaler(salerid, goodid, keys);
			System.out.println(System.currentTimeMillis() - start);
			while(itr.hasNext()){
				Result result = itr.next();
				long orderId = result.orderId();
				KeyValue[] kv = result.getAll();
				System.out.print(orderId + " ");
				for(KeyValue keyvalue:kv){
					System.out.print(keyvalue);
				}
				System.out.println();
			}
		}
	}
	
	static class SumOrdersByGood implements Runnable{

		private String goodid;
		private String key;
		public SumOrdersByGood(String goodid, String key) {
			super();
			this.goodid = goodid;
			this.key = key;
		}
		@Override
		public void run() {
			System.out.println("QuerySumOrderByGood" + orderSystem.sumOrdersByGood(goodid, key));
			System.out.println();
		}
		
	}
}	
