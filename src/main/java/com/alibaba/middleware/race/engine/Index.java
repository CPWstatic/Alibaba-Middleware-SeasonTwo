package com.alibaba.middleware.race.engine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

import com.alibaba.middleware.race.util.BTree;
import com.alibaba.middleware.race.util.MemoryBuffer;


/**
 * 
 * @author root
 *
 */
public class Index {
	
	private String indexName;
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
	public Index(String indexName, boolean isUnique, File indexDir,  Class clazz){
		try {
			this.isUnique = isUnique;
			this.indexName = indexName;
			this.index = new BTree(indexName,indexDir,clazz);
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
	 * @return 符合条件的地址列表
	 */
	public ArrayList<Long> get(Object key){
		//TODO 
		//BTree暂时不支持rangequery
		try{
		if(isUnique){
			Long value = 0L;
			
			value = index.get(key);

			if(value != null){
				ArrayList<Long> result = new ArrayList<Long>();
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
	 * @param key
	 * @return 符合条件的地址列表
	 */
	public ArrayList<Long> getWithCache(Object key){
		//TODO 
		//BTree暂时不支持rangequery
		try{
		if(isUnique){
			Long value = (long) 0;
			
			value = index.getWithCache(key);

			if(value != null){
				ArrayList<Long> result = new ArrayList<Long>();
				result.add(value);
				return result;
			}
			else{
				return null;
			}
		}else{
			return index.getAllWithCache(key);
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
	public boolean insert(Object key, Long value){
		try {
			index.insert(key, value);
			return true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} 
	}
	
	public BTree getIndex() {
		return index;
	}
	
	public String getIndexName(){
		return this.indexName;
	}
	
	public void saveIndex(File indexDirectory){
		try {
			MemoryBuffer unsafeBuffer = new MemoryBuffer(1024 * 512);
			this.index.saveAll(indexDirectory,unsafeBuffer);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
