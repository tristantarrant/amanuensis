/**
 * Copyright 2010 Tristan Tarrant
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dataforte.infinispan.amanuensis;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import net.dataforte.commons.collections.Computable;
import net.dataforte.commons.collections.Memoizer;
import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.backend.jgroups.JGroupsOperationDispatcher;
import net.dataforte.infinispan.amanuensis.backend.jgroups.JGroupsOperationProcessor;
import net.dataforte.infinispan.amanuensis.backend.lucene.LuceneOperationDispatcher;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.slf4j.Logger;

/**
 * AmanuensisManager is the entry point for obtaining {@link InfinispanIndexWriter} instances.
 * It handles synchronization between the nodes and matching between the writers and their
 * respective {@link InfinispanDirectory} instances. There should be only one instance of
 * AmanuensisManager for each {@link EmbeddedCacheManager}.
 * 
 * @author Tristan Tarrant
 *
 */
public class AmanuensisManager {
	private static final Logger log = LoggerFactory.make();
	private static final short INFINISPAN_INDEX_WRITER_SCOPE_ID = 1234;
	private static final Analyzer SIMPLE_ANALYZER = new SimpleAnalyzer();
	private final EmbeddedCacheManager cacheManager;
	private ConcurrentMap<String, InfinispanDirectory> directoryMap = new ConcurrentHashMap<String, InfinispanDirectory>();
	private Memoizer<String, InfinispanIndexWriter> writerMap;
	private JGroupsOperationProcessor remoteOperationProcessor;
	private OperationDispatcher remoteOperationDispatcher;
	private OperationDispatcher localOperationDispatcher;
	private Analyzer analyzer = SIMPLE_ANALYZER;

	/**
	 * Constructs an {@link AmanuensisManager} using the specified
	 * {@link EmbeddedCacheManager}
	 * 
	 * @param cacheManager
	 */
	public AmanuensisManager(EmbeddedCacheManager cacheManager) {
		if (cacheManager.getStatus()!=ComponentStatus.RUNNING) {
			throw new IllegalStateException("Cache is not running");
		}
		this.cacheManager = cacheManager;
		this.writerMap = new Memoizer<String, InfinispanIndexWriter>(new InfinispanIndexWriterMemoizer());
		this.remoteOperationProcessor = new JGroupsOperationProcessor(this, INFINISPAN_INDEX_WRITER_SCOPE_ID);
		this.remoteOperationDispatcher = new JGroupsOperationDispatcher(this, this.remoteOperationProcessor.getDispatcher());
		this.localOperationDispatcher = new LuceneOperationDispatcher(this);
	}

	public Analyzer getAnalyzer() {
		return analyzer;
	}

	/**
	 * Sets the {@link Analyzer} which will be used by the {@link IndexWriter}
	 *  
	 * @param analyzer
	 */
	public void setAnalyzer(Analyzer analyzer) {
		this.analyzer = analyzer;
	}

	/**
	 * Stops this instance from receiving/sending indexing jobs to other nodes.
	 * Should only be invoked just before stopping the underlying {@link EmbeddedCacheManager}
	 */
	public void close() {
		this.remoteOperationProcessor.close();
	}

	/**
	 * Retrieves (or initializes) an instance of InfinispanIndexWriter for the
	 * specified directory
	 * 
	 * @param directory
	 * @return
	 */
	public InfinispanIndexWriter getIndexWriter(InfinispanDirectory directory) throws IndexerException {
		if (directory.getIndexName() == null) {
			throw new IndexerException("InfinispanDirectory must not have a null indexName");
		}
		try {
			directoryMap.putIfAbsent(directory.getIndexName(), directory);
			return writerMap.compute(directory.getIndexName());
		} catch (Exception e) {
			log.error("Could not obtain an IndexWriter");
			throw new IndexerException(e);
		}
	}

	/**
	 * Returns the {@link InfinispanDirectory} for the specified indexName
	 * 
	 * @param indexName
	 * @return
	 */
	public InfinispanDirectory getDirectoryByIndexName(String indexName) {
		return directoryMap.get(indexName);
	}

	/**
	 * Returns the cluster's {@link Address} of the master
	 * @return
	 */
	public Address getMasterAddress() {
		return cacheManager.getCoordinator();
	}

	/**
	 * Returns the cluster's {@link Address} of the local node
	 * 
	 * @return
	 */
	public Address getLocalAddress() {
		return cacheManager.getAddress();
	}

	/**
	 * Dispatches the message to the appropriate destination depending on the
	 * role of this node.
	 * 
	 * @param indexOperations
	 * @throws IndexerException
	 */
	public void dispatchOperations(IndexOperations indexOperations) throws IndexerException {
		if (cacheManager.isCoordinator()) {
			// process the messages locally
			this.localOperationDispatcher.dispatch(indexOperations);
		} else {
			// send them to the remote
			this.remoteOperationDispatcher.dispatch(indexOperations);
		}
	}

	private class InfinispanIndexWriterMemoizer implements Computable<String, InfinispanIndexWriter> {
		@Override
		public InfinispanIndexWriter compute(String indexName) throws InterruptedException, ExecutionException {
			InfinispanIndexWriter iw = new InfinispanIndexWriter(AmanuensisManager.this, AmanuensisManager.this.directoryMap.get(indexName));			
			return iw;
		}

	}

}
