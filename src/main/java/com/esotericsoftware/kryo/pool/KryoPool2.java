package com.esotericsoftware.kryo.pool;

import java.lang.ref.SoftReference;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.esotericsoftware.kryo.Kryo;

/**
 * A simple, queue based pool for {@link Kryo} instances. Kryo instances
 * are cached using {@link SoftReference}s and therefore should not cause
 * OOMEs.
 * 
 * Usage:
 * <pre>
 * import com.esotericsoftware.kryo.Kryo;
 * import com.esotericsoftware.kryo.pool.KryoCallback;
 * import com.esotericsoftware.kryo.pool.KryoFactory;
 * import com.esotericsoftware.kryo.pool.KryoPool;
 * 
 * KryoFactory factory = new KryoFactory() {
 *   public Kryo create () {
 *     Kryo kryo = new Kryo();
 *     // configure kryo
 *     return kryo;
 *   }
 * };
 * KryoPool pool = new KryoPool(factory);
 * Kryo kryo = pool.borrow();
 * // do s.th. with kryo here, and afterwards release it
 * pool.release(kryo);
 * 
 * // or use a callback to work with kryo
 * String value = pool.run(new KryoCallback<String>() {
 *   public String execute(Kryo kryo) {
 *     return kryo.readObject(input, String.class);
 *   }
 * });
 *
 * </pre>
 * 
 * @author Martin Grotzke
 */
public class KryoPool2 {
	
	private final ThreadLocal<SoftReference<Kryo>> queue;
	private final KryoFactory factory;

	public KryoPool2(KryoFactory factory) {
		this(factory, new ThreadLocal<SoftReference<Kryo>>());
	}

	public KryoPool2(KryoFactory factory, ThreadLocal<SoftReference<Kryo>> queue) {
		this.factory = factory;
		this.queue = queue;
	}

	public Kryo borrow () {
		Kryo res;
		SoftReference<Kryo> ref;
		if((ref = queue.get()) != null && (res = ref.get()) != null) {
			return res;
		}
		return factory.create();
	}

	public void release (Kryo kryo) {
		Kryo res;
		SoftReference<Kryo> ref;
		if((ref = queue.get()) == null || (res = ref.get()) == null) {
			queue.set(new SoftReference(kryo));
		}
	}

	public <T> T run(KryoCallback<T> callback) {
		Kryo kryo = borrow();
		try {
			return callback.execute(kryo);
		} finally {
			release(kryo);
		}
	}

}
