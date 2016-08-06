package com.alibaba.middleware.race.util;

public class Entry<K, V> {
	K key;
	V value;
	boolean isFinished;

	public Entry(K key, V value) {
		this.key = key;
		this.value = value;
	}

	public Entry(boolean isFinished) {
		this.isFinished = isFinished;
	}
}