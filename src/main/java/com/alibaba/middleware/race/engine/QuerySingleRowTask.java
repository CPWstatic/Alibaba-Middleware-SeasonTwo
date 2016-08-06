package com.alibaba.middleware.race.engine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;

import com.alibaba.middleware.race.util.FileReader;

public 	class QuerySingleRowTask implements Callable{
	private Index index;
	private Object key;
	private String tablePath;
	private boolean isCached;
	public QuerySingleRowTask(Index index, String tablePath, Object key, boolean isCached){
		this.index = index;
		this.key = key;
		this.tablePath = tablePath;
		this.isCached = isCached;
	}
	@Override
	public Row call() throws Exception {
		ArrayList<Long> offsets = null;
		if(isCached){
			offsets = index.getWithCache(key);
		}
		else{
			offsets = index.get(key);
		}
		File file = new File(tablePath);
		if(offsets != null && offsets.size() == 1){
			try {
				return FileReader.fileRead(file, offsets.get(0));
			} catch (IOException e) {
				System.out.println(e.getMessage());
			}
		}
		
		return null;
	}
}
