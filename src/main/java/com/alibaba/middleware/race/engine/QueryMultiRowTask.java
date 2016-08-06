package com.alibaba.middleware.race.engine;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.Callable;

import com.alibaba.middleware.race.util.FileReader;

/**
 * 
 * @author root
 *
 */
public 	class QueryMultiRowTask implements Callable{
	private Index index;
	private Object key;
	private String tablePath;
	public QueryMultiRowTask(Index index, String tablePath, Object key){
		this.index = index;
		this.key = key;
		this.tablePath = tablePath;
	}
	@Override
	public ArrayList<Row> call() throws Exception {
		ArrayList<Row> rows = new ArrayList<Row>();
		ArrayList<Long> offsets = index.get(key);
		File file = new File(tablePath);
		if(offsets != null){
			for(Long offset:offsets){
				try{
					Row row = FileReader.fileRead(file, offset);	
					if(hook(row)){
						rows.add(row);
					}
				}catch(Exception e){
					System.out.println(e.getMessage());
				}
			}
		}
		
		return rows;
	}	
	
	protected boolean hook(Row row){
		return true;
	}
}
