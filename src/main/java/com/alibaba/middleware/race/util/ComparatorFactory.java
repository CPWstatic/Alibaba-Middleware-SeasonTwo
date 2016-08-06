package com.alibaba.middleware.race.util;

import java.util.Comparator;

public class ComparatorFactory {

	public static Comparator getComp(Class clazz){
		if(clazz == Long.class){
			return getLongComp();
		}
		
		if(clazz == String.class){
			return getStringComp();
		}
		
		return null;
	}
	
	private static Comparator getStringComp(){
		return new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}

		};
	}
	
	private static Comparator getLongComp(){
		return new Comparator<Long>() {
			@Override
			public int compare(Long o1, Long o2) {
				return o1.compareTo(o2);
			}
		};
	}
}
