package com.alibaba.middleware.race.engine;

/**
 * 
 * @author root
 *
 */
public interface WhereStrategy {
	boolean inRange(Row row);
}
