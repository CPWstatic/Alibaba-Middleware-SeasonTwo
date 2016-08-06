package com.alibaba.middleware.race.util;

import java.io.File;
import java.util.concurrent.Callable;

import com.alibaba.middleware.race.engine.Index;
import com.alibaba.middleware.race.engine.Table;

public class SaveIndexTask implements Callable{

	private Index index;
	private File orderIndexFile;
	public SaveIndexTask(Index index, String indexRootPath, Table table){
		table.setAnIndex(index.getIndexName(), index);
		this.orderIndexFile = new File(indexRootPath + index.getIndexName());
		if (!orderIndexFile.exists()) {
			orderIndexFile.mkdirs();
		}
		this.index = index;
	}
	
	@Override
	public Object call() throws Exception {
		index.saveIndex(orderIndexFile);
		System.out.println("**save done**");
		return true;
	}

}
