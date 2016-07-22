package pw.hellojava.middleware.race.engine;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @author root
 *
 */
public class Table {
	
	private String tableName;
	
	private String path;
	
	/**
	 * colomn信息,小表的列信息
	 */
	private ArrayList<String> colomns;
	
	/**
	 * k = colomn; v = Index
	 * 当前table索引
	 */
	private ConcurrentHashMap<String,Index> indexes;
	
	public Table(){
		
	}
	
	public Table(String tName, String path, ArrayList<String> colomns){
		this.tableName = tName;
		this.path = path;
		this.colomns = colomns;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public ArrayList<String> getColomns() {
		return colomns;
	}

	public void setColomns(ArrayList<String> colomns) {
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
	
	public String getPath(){
		return this.path;
	}
	
}
