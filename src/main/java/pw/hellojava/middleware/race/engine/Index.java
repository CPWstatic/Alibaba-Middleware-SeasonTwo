package pw.hellojava.middleware.race.engine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

import pw.hellojava.middlware.race.util.BTree;

public class Index {
	/**
	 * b树索引
	 */
	private BTree index;
	
	/**
	 * 指明是否是唯一索引
	 */
	private boolean isUnique;
	
	/**
	 * 
	 * @param indexName
	 * @param indexDir
	 * @param colomns
	 * @param comparator
	 */	
	public Index(String indexName, File indexDir,  Comparator comparator){
		try {
			this.index = new BTree(indexName,indexDir,comparator);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * key 代表要查找列的值
	 * @param key
	 * @return
	 */
	public ArrayList<Integer> get(Object key){
		//TODO 
		//BTree暂时不支持rangequery
		try{
		if(isUnique){
			Integer value = index.get(key);
			if(value != null){
				ArrayList<Integer> result = new ArrayList<Integer>();
				result.add(value);
				return result;
			}
			else{
				return null;
			}
		}else{
			return index.getAll(key);
		}
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * key 代表要查找列的值
	 * value 代表这个key在文件中的位置
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean insert(Object key, Integer value){
		try {
			index.insert(key, value);
			return true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} 
	}
}
