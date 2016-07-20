package pw.hellojava.middleware.race.engine;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class Database {
	
	private String databaseName;
	
	/**
	 * k=path,v=table
	 */
	private ConcurrentHashMap<String,Table> orderTables;
	
	/**
	 * 对于order大表，存储完整的列信息
	 */
	private ArrayList<Colomn> orderColomns;
	
	/**
	 * k=path,v=table
	 */
	private ConcurrentHashMap<String,Table> buyerTables;
	
	/**
	 * 对于buyer大表，存储完整的列信息
	 */
	private ArrayList<Colomn> buyerColomns;
	
	/**
	 * k=path,v=table
	 */
	private ConcurrentHashMap<String,Table> goodTables;
	
	/**
	 * 对于buyer大表，存储完整的列信息
	 */
	private ArrayList<Colomn> goodColomns;
}
