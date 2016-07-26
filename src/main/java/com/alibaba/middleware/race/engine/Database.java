package com.alibaba.middleware.race.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @author root
 *
 */
public class Database {
	
	private volatile static Database instance;
	
	private String databaseName;
	
	/**
	 * k=path,v=table
	 */
	private ConcurrentHashMap<String,Table> orderTables;
	
	/**
	 * 对于order大表，存储完整的列信息
	 */
	private ArrayList<String> orderColomns;
	
	/**
	 * k=path,v=table
	 */
	private ConcurrentHashMap<String,Table> buyerTables;
	
	/**
	 * 对于buyer大表，存储完整的列信息
	 */
	private ArrayList<String> buyerColomns;
	
	/**
	 * k=path,v=table
	 */
	private ConcurrentHashMap<String,Table> goodTables;
	
	/**
	 * 对于buyer大表，存储完整的列信息
	 */
	private ArrayList<String> goodColomns;
	
	private Database(){
		this.orderTables = new ConcurrentHashMap<String,Table>();
//		this.orderColomns = new ArrayList<Colomn>();
		this.buyerTables = new ConcurrentHashMap<String,Table>();
//		this.buyerColomns = new ArrayList<Colomn>();
		this.goodTables = new ConcurrentHashMap<String,Table>();
//		this.goodColomns = new ArrayList<Colomn>();
		this.databaseName = "default";
	}
	
	public static Database getInstance(String dbName){
		if(instance == null){
			synchronized(Database.class){
				if(instance == null)
					instance = new Database();
			}
		}
		
		return instance;
	}
	
	public void setGoodColomns(ArrayList<String> goodColomns) {
		this.goodColomns = goodColomns;
	}

	public Table setAnOrderTable(String tableName,Table table){
		return this.orderTables.put(tableName, table);
	}
	
	public void setOrderColomns(ArrayList<String> colomns){
		this.orderColomns = colomns;
	}
	
	public Table setABuyerTable(String tableName,Table table){
		return this.buyerTables.put(tableName, table);
	}
	
	public void setBuyerColomns(ArrayList<String> colomns){
		this.buyerColomns = colomns;
	}
	
	public Table setAGoodTable(String tableName,Table table){
		return this.goodTables.put(tableName, table);
	}
	
	public Map<String,Table> getOrderTables(){
		return this.orderTables;
	}
	
	public List<String> getOrderColomns(){
		return this.orderColomns;
	}
	
	public Map<String,Table> getBuyerTables(){
		return this.buyerTables;
	}
	
	public List<String> getBuyerColomns(){
		return this.buyerColomns;
	}
	
	public Map<String,Table> getGoodTables(){
		return this.goodTables;
	}
	
	public List<String>  getGoodColomns(){
		return this.goodColomns;
	}
}
