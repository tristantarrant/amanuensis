/**
 * Amanuensis, a distributed Lucene Index Writer for Infinispan
 *
 * Copyright (c) 2011, Tristan Tarrant
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */

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
import org.apache.lucene.index.BalancedSegmentMergePolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;

/**
 * The ExecutorContext contains all of the state associated with an executor.
 * 
 * @author Tristan Tarrant
 * 
 */
public class ExecutorContext {
	private static final Logger log = LoggerFactory.make();
	private static final String THREAD_GROUP_PREFIX = "Amanuensis: ";
	private static final int QUEUE_MAX_LENGTH = 1000;
	private static final IndexWriter.MaxFieldLength MAX_FIELD_LENGTH = new IndexWriter.MaxFieldLength(IndexWriter.DEFAULT_MAX_FIELD_LENGTH);
	private final ExecutorService executor;
	private LuceneOperationExecutorFactory operationExecutorFactory;
	private final Directory directory;
	private IndexWriter writer;
	private Analyzer analyzer;
	private AmanuensisManager manager;

	public ExecutorContext(AmanuensisManager manager, Directory directory, Analyzer analyzer) {
		this.manager = manager;
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

	public Directory getDirectory() {
		return directory;
	}

	public synchronized IndexWriter getWriter() throws IndexerException {
		if (writer != null)
			return writer;
		try {
			writer = new IndexWriter(directory, analyzer, true, MAX_FIELD_LENGTH);
			manager.getWriterConfigurator().configure(writer);
		} catch (IOException e) {
			writer = null;
			throw new IndexerException("Error while creating writer for index " + AmanuensisManager.getUniqueDirectoryIdentifier(directory), e);
		}
		return writer;
	}

	public synchronized void commit() throws IndexerException {
		if (writer != null) {
			try {
				writer.commit();
			} catch (IOException e) {
				throw new IndexerException("Error while committing writer for index " + AmanuensisManager.getUniqueDirectoryIdentifier(directory), e);
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
				log.error("Error while closing writer for index " + AmanuensisManager.getUniqueDirectoryIdentifier(directory), e);
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
			log.warn("Error while unlocking writer for index " + AmanuensisManager.getUniqueDirectoryIdentifier(directory), e);
		}
	}

	/**
	 * Provide an implementation of {@link ThreadFactory} which gives sensible names to threads
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
	 * Provide an implementation of {@link RejectedExecutionHandler} which queues up threads
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
}
