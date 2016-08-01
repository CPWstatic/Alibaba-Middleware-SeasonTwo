package com.alibaba.middlware.race.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;



/**
 * AsyncTaskService<Task> - 基础服务泛型抽象类
 *
 * @param <Task>    - 任务类型
 */
public abstract class AsyncTaskService<Task>
{
    /**
     * 任务队列
     */
    private BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<Task>();
    
    /**
     * 队列同步锁
     */
    private final Object queueLock = new Object();
    
    /**
     * 等待任务到达的超时时间
     */
    protected long waitTimeout = 1000L;

    /**
     * 任务队列中，任务数量到达多少时触发处理
     */
    protected int notifyAtCount = 0;

    /**
     * 队列告警阈值
     */
    protected int queueWarnThreshold = 1000;

    /**
     * 服务线程
     */
    private Thread serviceThread = null;
    private FutureTask<Long> ft = null;
    protected long lastNumberSize = 0L;


    public AsyncTaskService()
    {
    	this.ft = new FutureTask<Long>(this.monitor);
        this.serviceThread = new Thread(ft);
    }


    /**
     * 任务处理的抽象方法
     * @param taskList
     */
    protected abstract void handler(Task taskList);

    public long waitingSize()
    {
        long waitingSize = 0L;
        
        synchronized(this.queueLock)
        {
            waitingSize = this.taskQueue.size();
        }
        
        return waitingSize;
    }
    
    /**
     * 提交任务请求
     * @param task
     */
    public void put(Task task)
    {
        try {
			this.taskQueue.put(task);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }


    /**
     * 临时的任务列表
     */
//    private List<Task> taskList = new ArrayList<Task>();

    /**
     * 任务队列监听
     */
    protected void doQueueMonitor()
    {
    	try {
			this.handler(this.taskQueue.take());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    
    /**
     * 服务线程执行体
     */
    private Callable monitor = new Callable()
    {
        
        
        @Override
        public Long call()
        {
            while(AsyncTaskService.this.serviceRunning)
            {
                AsyncTaskService.this.doQueueMonitor();
            }
            
            return lastNumberSize;
        }
    };


    /**
     * 服务运行标记
     */
    protected volatile boolean serviceRunning = false;

    public boolean isRunning()
    {
        return this.serviceRunning;
    }

    
    /**
     * 启动服务
     */
    public void startup()
    {
        if(!this.serviceRunning)
        {
            this.serviceRunning = true;
            this.serviceThread.start();
        }

//		Logger.trace(this.getClass(), "服务线程已启动.");
    }

    /**
     * 停止服务
     */
    public void shutdown()
    {
        if(this.serviceRunning)
        {
            this.serviceRunning = false;
    
            synchronized(this.queueLock)
            {
                this.queueLock.notify();
            }
    
            //
            // 等待服务线程终止
            //
            try
            {
                this.serviceThread.join(1000L);
            }
            catch (InterruptedException e)
            {
                this.serviceThread.interrupt();
            }
        }

//		Logger.trace(this.getClass(), "服务线程已终止.");
    }
    
    
    protected void delay(long timeout)
    {
        try
        {
            Thread.sleep(timeout);
        }
        catch (InterruptedException e)
        {
        }
    }
    
    protected void printf(String format, Object...args)
    {
        String message = String.format(format, args);
        System.out.println(message);
    }
    
    protected void printf(String message)
    {
        System.out.println(message);
    }

}
