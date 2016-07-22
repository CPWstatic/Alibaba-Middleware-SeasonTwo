package com.alibaba.middleware.race.imp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;

import com.alibaba.middleware.race.KV;
import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.ResultImp;


import pw.hellojava.middleware.race.engine.Database;
import pw.hellojava.middleware.race.engine.Row;
import pw.hellojava.middleware.race.engine.Table;
import pw.hellojava.middlware.race.util.FileReader;

/**
 * 
 * @author root
 *
 */
public class OrderSystemImp implements OrderSystem{
	
	private Database db = Database.getInstance(null);

	public OrderSystemImp(){
		
	}
	
	@Override
	public void construct(Collection<String> orderFiles, Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public Result queryOrder(long orderId, Collection<String> keys) {
		ResultImp result = null;
		Row orderRow = null;
		Row buyerRow = null;
		Row goodRow = null;
		
		if(keys !=null && keys.size() == 0){
			//如果为空，则排除所有字段
			return ResultImp.createNoRow(orderId);
		}
		
		for(Entry<String,Table> entry : db.getOrderTables().entrySet()){
			Table table = entry.getValue();
			ArrayList<Long> offsets = table.getAnIndex("orderid").get(orderId);
			if(offsets != null && offsets.size() == 1){
				File file = new File(table.getPath()+table.getTableName());
				try {
					orderRow = FileReader.fileRead(file, offsets.get(0));
				} catch (IOException e) {
					
				}
			}
		}
		
		if(orderRow == null){
			//如果该订单不存在，返回null
			return null;
		}
		
		buyerRow = this.joinBuyerQuery(orderRow.get("buyerid").valueAsString());

		goodRow = this.joinGoodQuery(orderRow.get("goodid").valueAsString());
		
		
		if(keys == null){
			//待查询的字段，如果为null，则查询所有字段
			return ResultImp.createResultRow(orderRow, buyerRow, goodRow);
		}else{
			return ResultImp.createResultRow(orderRow, buyerRow, goodRow, keys);
		}
	}

	@Override
	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
		if(buyerid == null){
			return null;
		}
		
		ArrayList<Row> orderRows = new ArrayList<Row>();
		ArrayList<Result> results = new ArrayList<Result>();
		
		//从orders中查出所有符合条件的数据
		for(Entry<String,Table> entry : db.getOrderTables().entrySet()){
			Table table = entry.getValue();
			ArrayList<Long> offsets = table.getAnIndex("orderid").get(buyerid);
			File file = new File(table.getPath()+table.getTableName());
			if(offsets != null){
				for(Long offset:offsets){
					try{
						Row orderRow = FileReader.fileRead(file, offset);
						long createTime = orderRow.get("createtime").valueAsLong();
						if( createTime >= startTime && createTime < endTime){
							orderRows.add(orderRow);
						}
					}catch(Exception e){
						
					}
				}
			}
		}
		
		if(orderRows.size() == 0){
			//如果该buyerid不在order中，返回什么
			//TODO
		}
		
		//join操作
		// 已知buyer，只用查询一次
		Row buyerRow = this.joinBuyerQuery(buyerid);
		//
		for(Row orderRow : orderRows){
			Row goodRow = this.joinGoodQuery(orderRow.get("goodid").valueAsString());
			//默认join所有
			results.add(ResultImp.createResultRow(orderRow, buyerRow, goodRow));
		}
		
		//按照createtime大到小排列
		Collections.sort(results,new Comparator<Result>(){

			@Override
			public int compare(Result o1, Result o2) {
				Long c1=(long) -1,c2=(long) -1;
				try{
					c1 = o1.get("createtime").valueAsLong();
					c2 = o2.get("createtime").valueAsLong();
				}catch(Exception e){
					
				}
				return  c1>c2  ? -1 : 1;
			}
			
		});
		
		return results.iterator();
	}

	@Override
	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
		
		ArrayList<Row> orderRows = new ArrayList<Row>();
		ArrayList<Result> results = new ArrayList<Result>();

		//按照goodid查询，用salerid过滤
		for(Entry<String,Table> entry : db.getOrderTables().entrySet()){
			Table table = entry.getValue();
			ArrayList<Long> offsets = table.getAnIndex("orderid").get(goodid);
			File file = new File(table.getPath()+table.getTableName());
			if(offsets != null){
				for(Long offset:offsets){
					try{
						Row orderRow = FileReader.fileRead(file, offset);
						String saleridQ = orderRow.get("salerid").valueAsString();
						if(saleridQ.equals(salerid)){
							orderRows.add(orderRow);
						}
					}catch(Exception e){
						
					}
				}
			}
		}
		
		if(keys !=null && keys.size() == 0){
			//如果为空，则排除所有字段
			for(Row orderRow : orderRows){
				try {
					results.add(ResultImp.createNoRow(orderRow.get("orderid").valueAsLong()));
				} catch (TypeException e) {
					
				}
			}
			return results.iterator();
		}
		
		//join操作
		//已知goodid，只用查询一次
		Row goodRow = this.joinGoodQuery(goodid);
		//
		for(Row orderRow : orderRows){
			Row buyerRow = this.joinBuyerQuery(orderRow.get("buyerid").valueAsString());
			
			if(keys == null){
				//待查询的字段，如果为null，则查询所有字段
				results.add(ResultImp.createResultRow(orderRow, buyerRow, goodRow));
			}else{
				results.add(ResultImp.createResultRow(orderRow, buyerRow, goodRow, keys));
			}
		}
		
		//按照订单id从小至大排序
		Collections.sort(results,new Comparator<Result>(){

			@Override
			public int compare(Result o1, Result o2) {
				Long c1=(long) -1,c2=(long) -1;
				try{
					c1 = o1.get("orderid").valueAsLong();
					c2 = o2.get("orderid").valueAsLong();
				}catch(Exception e){
					
				}
				return  c1>c2  ? 1 : -1;
			}
			
		});
		
		return results.iterator();
	}

	@Override
	public KeyValue sumOrdersByGood(String goodid, String key) {
		boolean joinBuyer = false;
		boolean joinGood = false;
		if(!db.getOrderColomns().contains(key)){
			if(db.getBuyerColomns().contains(key)){
				joinBuyer = true;
			}else if(db.getGoodColomns().contains(key)){
				joinGood = true;
			}else{
				// 如果查询订单中的所有商品均不包含该字段，则返回null
				return null;
			}
		}
		
		ArrayList<Row> orderRows = new ArrayList<Row>();
		ArrayList<Result> results = new ArrayList<Result>();

		for(Entry<String,Table> entry : db.getOrderTables().entrySet()){
			Table table = entry.getValue();
			ArrayList<Long> offsets = table.getAnIndex("orderid").get(goodid);
			File file = new File(table.getPath()+table.getTableName());
			if(offsets != null){
				for(Long offset:offsets){
					try{
						Row orderRow = FileReader.fileRead(file, offset);
						orderRows.add(orderRow);						
					}catch(Exception e){
						
					}
				}
			}
		}
		
		//如果求和的key中包含非long/double类型字段，则返回null
		if(!(joinBuyer || joinGood)){
			return new KV(goodid,sumUp(orderRows,key));
		}else if(joinBuyer){
			ArrayList<Row> buyerRows = new ArrayList<Row>();
			for(Row orderRow : orderRows){
				Row buyerRow = this.joinBuyerQuery(orderRow.get("buyerid").valueAsString());
				buyerRows.add(buyerRow);
			}
			return new KV(goodid,sumUp(buyerRows,key));
		}else if(joinGood){
			Row goodRow = this.joinBuyerQuery(goodid);

			ArrayList<Row> goodRows = new ArrayList<Row>();
			for(int i = 0; i < orderRows.size(); i++){
				goodRows.add(goodRow);
			}
			
			return new KV(goodid,sumUp(goodRows,key));
		}
		
		return null;
	}
	
	public String sumUp(ArrayList<Row> rows , String key){
		Double dsum = 0.0;
		Long lsum = (long) 0;
		boolean isLong = false;
		boolean isDouble = false;
		for(Row row : rows){
			KV kv = row.get(key);
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
		}else{
			return dsum.toString();
		}
	}
	
	private Row joinBuyerQuery(String colomnValue){
		//未找到返回null
		for(Entry<String,Table> entry : db.getBuyerTables().entrySet()){
			Table table = entry.getValue();
			ArrayList<Long> offsets = table.getAnIndex("buyerid").get(colomnValue);
			if(offsets != null && offsets.size() == 1){
				File file = new File(table.getPath()+table.getTableName());
				try {
					return FileReader.fileRead(file, offsets.get(0));
				} catch (IOException e) {
					
				}
			}
		}

		return null;
	}
	
	private Row joinGoodQuery(String colomnValue){
		for(Entry<String,Table> entry : db.getGoodTables().entrySet()){
			Table table = entry.getValue();
			ArrayList<Long> offsets = table.getAnIndex("goodid").get(colomnValue);
			if(offsets != null && offsets.size() == 1){
				File file = new File(table.getPath()+table.getTableName());
				try {
					return FileReader.fileRead(file, offsets.get(0));
				} catch (IOException e) {
					
				}
			}
		}
		
		return null;
	}

}