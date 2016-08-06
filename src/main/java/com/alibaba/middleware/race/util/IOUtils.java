package com.alibaba.middleware.race.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

public class IOUtils {
	public static LineIterator lineIterator(final InputStream input, final String encoding)
			throws IllegalArgumentException, UnsupportedEncodingException {
		return new LineIterator(new InputStreamReader(input, encoding));
	}

	public static LineIterator lineIterator(final Reader reader) {
		return new LineIterator(reader);
	}

	public static void closeQuietly(final Reader input) {
		closeQuietly((Closeable) input);
	}

	public static void closeQuietly(final Closeable closeable) {
		try {
			if (closeable != null) {
				closeable.close();
			}

		} catch (final IOException ioe) {
			// ignore
		}
	}

}
