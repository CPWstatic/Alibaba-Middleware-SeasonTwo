package com.alibaba.middleware.race.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.alibaba.middleware.race.engine.Row;

public class FileReader {

	public static Row fileRead(File file, long offset) throws IOException {
		Row row = new Row();
		RandomAccessFile ran = new RandomAccessFile(file, "r");
		ran.seek(offset);
		String line = new String(ran.readLine().getBytes("iso8859-1"), "utf-8");
//		System.out.println(line);
		ran.close();
		
		String[] key_values = line.split("\t");
		for (String key_value : key_values) {
			String[] kv = key_value.split(":");
			row.put(kv[0], kv[1]);
		}
		
		return row;
	}
}
