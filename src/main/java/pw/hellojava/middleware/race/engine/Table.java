package pw.hellojava.middleware.race.engine;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class Table {
	
	private String tableName;
	
	/**
	 * colomn信息,小表的列信息
	 */
	private ArrayList<Colomn> colomns;
	
	/**
	 * 当前table索引
	 */
	private ConcurrentHashMap<Colomn,Index> indexes;
	
	public Table(){
		
	}
	
	public Table(String tName,ArrayList<Colomn> colomns){
		this.tableName = tName;
		this.colomns = colomns;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public ArrayList<Colomn> getColomns() {
		return colomns;
	}

	public void setColomns(ArrayList<Colomn> colomns) {
		this.colomns = colomns;
	}

	public ConcurrentHashMap<Colomn, Index> getIndexes() {
		return indexes;
	}

	public void setAnIndex(Colomn colomn, Index index) {
		this.indexes.put(colomn, index);
	}
	
}
