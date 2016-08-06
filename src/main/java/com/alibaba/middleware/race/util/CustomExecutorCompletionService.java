package com.alibaba.middleware.race.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

public class CustomExecutorCompletionService<V> implements CompletionService<V> {

    private final Executor disk1Exe;
    private final Executor disk2Exe;
    private final Executor disk3Exe;
//    private final AbstractExecutorService aes;
    private final BlockingQueue<Future<V>> completionQueue;

    /**
     * FutureTask extension to enqueue upon completion
     */
    private class QueueingFuture extends FutureTask<Void> {
        QueueingFuture(RunnableFuture<V> task) {
            super(task, null);
            this.task = task;
        }
        protected void done() { completionQueue.add(task); }
        private final Future<V> task;
    }

    private RunnableFuture<V> newTaskFor(Callable<V> task) {
            return new FutureTask<V>(task);
    }

    private RunnableFuture<V> newTaskFor(Runnable task, V result) {
            return new FutureTask<V>(task, result);
    }

    /**
     * Creates an ExecutorCompletionService using the supplied
     * executor for base task execution and a
     * {@link LinkedBlockingQueue} as a completion queue.
     *
     * @param executor the executor to use
     * @throws NullPointerException if executor is {@code null}
     */
    public CustomExecutorCompletionService(Executor disk1Exe,Executor disk2Exe,Executor disk3Exe) {
        if (disk1Exe == null || disk2Exe == null || disk3Exe == null )
            throw new NullPointerException();
        this.disk1Exe = disk1Exe;
        this.disk2Exe = disk2Exe;
        this.disk3Exe = disk3Exe;
        this.completionQueue = new LinkedBlockingQueue<Future<V>>();
    }

    public Future<V> submit(Callable<V> task,String disk) {
        if (task == null) throw new NullPointerException();
        RunnableFuture<V> f = newTaskFor(task);
        if(disk.equals("disk1"))
        	disk1Exe.execute(new QueueingFuture(f));
        if(disk.equals("disk2"))
        	disk2Exe.execute(new QueueingFuture(f));
        if(disk.equals("disk3"))
        	disk3Exe.execute(new QueueingFuture(f));
        return f;
    }

    public Future<V> take() throws InterruptedException {
        return completionQueue.take();
    }

    public Future<V> poll() {
        return completionQueue.poll();
    }

    public Future<V> poll(long timeout, TimeUnit unit)
            throws InterruptedException {
        return completionQueue.poll(timeout, unit);
    }

	@Override
	public Future<V> submit(Callable<V> task) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Future<V> submit(Runnable task, V result) {
		// TODO Auto-generated method stub
		return null;
	}

}
