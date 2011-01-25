package net.dataforte.infinispan.amanuensis.backend.elasticsearch;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.dataforte.commons.slf4j.LoggerFactory;

import org.elasticsearch.client.Client;
import org.slf4j.Logger;

public class ElasticSearchExecutorContext {
	private static final Logger log = LoggerFactory.make();
	private static final String THREAD_GROUP_PREFIX = "Amanuensis: ";
	private static final int QUEUE_MAX_LENGTH = 1000;
	private Client client;
	private final ExecutorService executor;
	private ElasticSearchOperationExecutorFactory operationExecutorFactory;

	public ElasticSearchExecutorContext(String indexName) {
		this.executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(QUEUE_MAX_LENGTH), new ExecutorThreadFactory("IndexWriter"), new BlockingPolicy());
		client = nodeBuilder().data(true).node().client();
		operationExecutorFactory = new ElasticSearchOperationExecutorFactory();
	}

	public synchronized void close() {
		client.close();
	}

	public Client getClient() {
		return client;
	}

	/**
	 * Provide an implementation of {@link ThreadFactory} which gives sensible
	 * names to threads
	 * 
	 * @author Tristan Tarrant
	 * @author Sanne Grinovero
	 */
	private static class ExecutorThreadFactory implements ThreadFactory {
		final ThreadGroup group;
		final AtomicInteger threadNumber = new AtomicInteger(1);
		final String namePrefix;

		ExecutorThreadFactory(String groupname) {
			SecurityManager s = System.getSecurityManager();
			group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
			namePrefix = THREAD_GROUP_PREFIX + groupname + "-";
		}

		public Thread newThread(Runnable r) {
			Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
			return t;
		}

	}

	/**
	 * Provide an implementation of {@link RejectedExecutionHandler} which
	 * queues up threads
	 * 
	 * @author Tristan Tarrant
	 * @author Sanne Grinovero
	 */
	public static class BlockingPolicy implements RejectedExecutionHandler {
		public BlockingPolicy() {
		}

		public void rejectedExecution(Runnable r, ThreadPoolExecutor exec) {
			try {
				exec.getQueue().put(r);
			} catch (InterruptedException e) {
				log.error("Work discarded, thread was interrupted while waiting for space to schedule: {}", r);
			}
		}
	}

	public ExecutorService getExecutor() {
		return executor;
	}

	public ElasticSearchOperationExecutorFactory getOperationExecutorFactory() {
		return operationExecutorFactory;
	}
	
	
}
