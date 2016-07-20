package pw.hellojava.middleware.race.engine;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class Table {
	
	private String tableName;
	
	/**
	 * colomn信息
	 */
	private ArrayList<Colomn> colomns;
	
	/**
	 * 当前table索引
	 */
	private ConcurrentHashMap<Colomn,Index> indexes;
}
