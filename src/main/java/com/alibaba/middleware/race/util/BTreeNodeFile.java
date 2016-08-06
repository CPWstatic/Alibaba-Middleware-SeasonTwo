package com.alibaba.middleware.race.util;

import java.io.Serializable;
import java.util.ArrayList;

public class BTreeNodeFile implements Serializable{
	@Override
	public String toString() {
		return "BTreeNodeFile [keys=" + keys + ", values=" + values + ", childNodeIds=" + childNodeIds + "]";
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -3417293866701822750L;

	public BTreeNodeFile(){
		
	}
	
	public BTreeNodeFile(ArrayList<Object> keys, ArrayList<Long> values, ArrayList<Integer> childNodeIds) {
		super();
		this.keys = keys;
		this.values = values;
		this.childNodeIds = childNodeIds;
	}

	/**
	 * keys 关键字
	 * values 地址
	 */
	private ArrayList<Object> keys;
	private ArrayList<Long> values;
	
	/**
	 * childNodeIds 为子女们编号，作为指针，通过编号查找子女
	 * loadedChildren 加载的子女们
	 */
	private ArrayList<Integer> childNodeIds;
	


	public ArrayList<Object> getKeys() {
		return keys;
	}

	public void setKeys(ArrayList<Object> keys) {
		this.keys = keys;
	}

	public ArrayList<Long> getValues() {
		return values;
	}

	public void setValues(ArrayList<Long> values) {
		this.values = values;
	}

	public ArrayList<Integer> getChildNodeIds() {
		return childNodeIds;
	}

	public void setChildNodeIds(ArrayList<Integer> childNodeIds) {
		this.childNodeIds = childNodeIds;
	}
	
}
