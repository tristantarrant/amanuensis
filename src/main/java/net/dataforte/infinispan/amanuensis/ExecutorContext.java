package net.dataforte.infinispan.amanuensis;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.backend.lucene.LuceneOperationExecutorFactory;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.infinispan.lucene.InfinispanDirectory;
import org.slf4j.Logger;

public class ExecutorContext {
	private static final Logger log = LoggerFactory.make();
	private static final String THREAD_GROUP_PREFIX = "Amanuensis: ";
	private static final int QUEUE_MAX_LENGTH = 1000;
	private static final IndexWriter.MaxFieldLength MAX_FIELD_LENGTH = new IndexWriter.MaxFieldLength(IndexWriter.DEFAULT_MAX_FIELD_LENGTH);
	private final ExecutorService executor;
	private LuceneOperationExecutorFactory operationExecutorFactory;
	private final InfinispanDirectory directory;
	private IndexWriter writer;
	private Analyzer analyzer;

	public ExecutorContext(InfinispanDirectory directory, Analyzer analyzer) {
		this.executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(QUEUE_MAX_LENGTH), new ExecutorThreadFactory("IndexWriter"), new BlockingPolicy());		
		this.directory = directory;
		this.analyzer = analyzer;
		this.operationExecutorFactory = new LuceneOperationExecutorFactory();
	}

	public ExecutorService getExecutor() {
		return executor;
	}

	public LuceneOperationExecutorFactory getOperationExecutorFactory() {
		return operationExecutorFactory;
	}

	public void setOperationExecutorFactory(LuceneOperationExecutorFactory operationExecutorFactory) {
		this.operationExecutorFactory = operationExecutorFactory;
	}

	public InfinispanDirectory getDirectory() {
		return directory;
	}

	public synchronized IndexWriter getWriter() throws IndexerException {
		if (writer != null)
			return writer;
		try {
			writer = new IndexWriter(directory, analyzer, true, MAX_FIELD_LENGTH);
		} catch (IOException e) {
			writer = null;
			throw new IndexerException("Error while creating writer for index " + directory.getIndexName(), e);
		}
		return writer;
	}

	public synchronized void commit() throws IndexerException {
		if (writer != null) {
			try {
				writer.commit();
			} catch (IOException e) {
				throw new IndexerException("Error while committing writer for index " + directory.getIndexName(), e);
			}
		}
	}

	public synchronized void close() {
		IndexWriter w = writer;
		writer = null;
		if (w != null) {
			try {
				w.close();
			} catch (IOException e) {
				log.error("Error while closing writer for index " + directory.getIndexName(), e);
			}
		}
	}

	public synchronized void forceUnlock() {
		try {
			try {
				close();
			} finally {
				IndexWriter.unlock(this.directory);
			}
		} catch (Exception e) {
			log.warn("Error while unlocking writer for index " + directory.getIndexName(), e);
		}
	}

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
}
