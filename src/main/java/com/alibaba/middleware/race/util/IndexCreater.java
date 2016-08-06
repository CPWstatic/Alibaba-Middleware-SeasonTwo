package com.alibaba.middleware.race.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import com.alibaba.middleware.race.engine.Index;
import com.alibaba.middleware.race.engine.Table;

public class IndexCreater{
	
    private final ThreadPoolExecutor disk1Exe;
    private final ThreadPoolExecutor disk2Exe;
    private final ThreadPoolExecutor disk3Exe;
    /**
     * 任务队列
     */
    private BlockingQueue<Object[]> taskQueue = new LinkedBlockingQueue<Object[]>(1000);

    /**
     * 服务线程
     */
    private Thread serviceThread = null;
    
    private FutureTask<Long> ft = null;
    protected long lastNumberSize = 0L;
    private Table table;
	private Index index;
	private ArrayList<Future<Object>> saveFutures;
	private ArrayList<String> storeFolders;
	
	private int count;
	public IndexCreater(Index index,ThreadPoolExecutor disk1Exe,ThreadPoolExecutor disk2Exe,ThreadPoolExecutor disk3Exe,Table table,ArrayList<Future<Object>> saveFutures,ArrayList<String> storeFolders){
		this.index = index;
		this.disk1Exe = disk1Exe;
        this.disk2Exe = disk2Exe;
        this.disk3Exe = disk3Exe;
        this.table = table;
        this.saveFutures = saveFutures;
        this.storeFolders = storeFolders;
		this.ft = new FutureTask<Long>(this.monitor);
        this.serviceThread = new Thread(ft);
	}
	

	protected void handler(Object[] task) {
		
		if(task != null){
			count++;
			index.insert(task[0], (long)task[1]);
		}
//		lastNumberSize += (sentence.getBytes().length + 1);
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

		this.handler(this.taskQueue.poll());
		
    }
    
    /**
     * 提交任务请求
     * @param task
     */
    public void put(Object[] task)
    {
        try {
			this.taskQueue.put(task);
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
            while(IndexCreater.this.serviceRunning || IndexCreater.this.taskQueue.size() > 0)
            {
            	IndexCreater.this.doQueueMonitor();
            }
//            System.out.println("--insert done--: " + index.getIndexName() + "insert count:" + count);
            //结束，提交保存任务
            saveindex();
            
            return lastNumberSize;
        }
    };
    
    private void saveindex(){
    	if(this.table.getPath().startsWith("/disk1")){
    		System.out.println("disk2 queue size:" + disk2Exe.getQueue().size() + " disk3 queue size:" + disk3Exe.getQueue().size());
    		if(disk2Exe.getActiveCount() > disk3Exe.getActiveCount() || disk2Exe.getQueue().size() > disk3Exe.getQueue().size()){
    			this.saveFutures.add(disk3Exe.submit(new SaveIndexTask(index,this.storeFolders.get(2),table)));
//    			System.out.println("disk1 sumbmit 3");
    		}else{
    			this.saveFutures.add(disk2Exe.submit(new SaveIndexTask(index,this.storeFolders.get(1),table)));
//    			System.out.println("disk1 sumbmit 2");
    		}
    	}else if(this.table.getPath().startsWith("/disk2")){
    		System.out.println("disk1 queue size:" + disk1Exe.getQueue().size() + " disk3 queue size:" + disk3Exe.getQueue().size());
    		if(disk1Exe.getActiveCount() > disk3Exe.getActiveCount() || disk1Exe.getQueue().size() > disk3Exe.getQueue().size()){
    			this.saveFutures.add(disk3Exe.submit(new SaveIndexTask(index,this.storeFolders.get(2),table)));
//    			System.out.println("disk2 sumbmit 3");
    		}else{
    			this.saveFutures.add(disk1Exe.submit(new SaveIndexTask(index,this.storeFolders.get(0),table)));
//    			System.out.println("disk2 sumbmit 1");
    		}
    	}else if(this.table.getPath().startsWith("/disk3")){
    		System.out.println("disk2 queue size:" + disk2Exe.getQueue().size() + " disk2 queue size:" + disk2Exe.getQueue().size());
    		if(disk1Exe.getActiveCount() > disk2Exe.getActiveCount() || disk1Exe.getQueue().size() > disk2Exe.getQueue().size()){
    			this.saveFutures.add(disk2Exe.submit(new SaveIndexTask(index,this.storeFolders.get(1),table)));
//    			System.out.println("disk3 sumbmit 2");
    		}else{
    			this.saveFutures.add(disk1Exe.submit(new SaveIndexTask(index,this.storeFolders.get(0),table)));
//    			System.out.println("disk3 sumbmit 1");
    		}
    	}
    	
    }
    
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
    public Future<Long> startup()
    {
        if(!this.serviceRunning)
        {
            this.serviceRunning = true;
            this.serviceThread.start();
        }
        
        return this.ft;

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
    
//            synchronized(this.queueLock)
//            {
//                this.queueLock.notify();
//            }
//    
            //
            // 等待服务线程终止
            //
//            try
//            {
//                this.serviceThread.join(1000L);
//            }
//            catch (InterruptedException e)
//            {
//                this.serviceThread.interrupt();
//            }
        }

//		Logger.trace(this.getClass(), "服务线程已终止.");
    }


}
