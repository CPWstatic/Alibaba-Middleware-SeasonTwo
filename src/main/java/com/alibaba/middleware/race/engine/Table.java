package com.alibaba.middleware.race.engine;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @author root
 *
 */
public class Table {
	
	private String tableName;
	
	private String path;
	
	public String getPath() {
		return path;
	}


	/**
	 * colomn信息,小表的列信息
	 */
	private HashSet<String> colomns;
	
	/**
	 * k = colomn; v = Index
	 * 当前table索引
	 */
	private ConcurrentHashMap<String,Index> indexes;
	
	public Table(){
		
	}
	
	public Table(String tName, String path, HashSet<String> colomns){
		this.tableName = tName;
		this.path = path;
		this.colomns = colomns;
		this.indexes = new ConcurrentHashMap<String, Index>();
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public HashSet<String> getColomns() {
		return colomns;
	}

	public void setColomns(HashSet<String> colomns) {
		this.colomns = colomns;
	}

	public ConcurrentHashMap<String, Index> getIndexes() {
		return indexes;
	}
	
	public Index getAnIndex(String string){
		return this.indexes.get(string);
	}

	public void setAnIndex(String colomn, Index index) {
		this.indexes.put(colomn, index);
	}
	
}
