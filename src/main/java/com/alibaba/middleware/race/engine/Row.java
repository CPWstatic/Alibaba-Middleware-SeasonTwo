package com.alibaba.middleware.race.engine;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.KV;

/**
 * 
 * @author root
 *
 */
public class Row {
	private HashMap<String,KV> row;
	
	public Row(){
		this.row = new HashMap<String,KV>();
	}
	
	public KV get(String key){
		return key == null? null:this.row.get(key);
	}
	
	public Row put(String key, String value){
		KV kv =  new KV(key,value);
		this.row.put(key, kv);
		return this;
	}
	public Row put(String key, Long value){
		KV kv = new KV(key,value.toString());
		this.row.put(key, kv);
		return this;
	}
	
	public void putAll(Map<String,KV> m){
		this.row.putAll(m);
	}
	
	public Map<String,KV> getAll(){
		return this.row;
	}
	
	public Collection<KV> getValues(){
		return this.row.values();
	}

}
