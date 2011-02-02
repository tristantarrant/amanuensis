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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import net.dataforte.commons.collections.Computable;
import net.dataforte.commons.collections.Memoizer;
import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.backend.jgroups.JGroupsOperationDispatcher;
import net.dataforte.infinispan.amanuensis.backend.jgroups.JGroupsOperationReceiver;
import net.dataforte.infinispan.amanuensis.backend.lucene.LuceneOperationDispatcher;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.slf4j.Logger;

/**
 * AmanuensisManager is the entry point for obtaining {@link AmanuensisIndexWriter} instances.
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
	private ConcurrentMap<String, Directory> directoryMap = new ConcurrentHashMap<String, Directory>();
	private Memoizer<String, AmanuensisIndexWriter> writerMap;
	private Memoizer<String, AmanuensisIndexReader> readerMap;
	private JGroupsOperationReceiver remoteOperationProcessor;
	private OperationDispatcher remoteOperationDispatcher;
	private OperationDispatcher localOperationDispatcher;
	private WriterConfigurator writerConfigurator = new DefaultWriterConfigurator();
	private Analyzer analyzer = SIMPLE_ANALYZER;

	/**
	 * Constructs an {@link AmanuensisManager} using the specified
	 * {@link EmbeddedCacheManager}
	 * 
	 * @param cacheManager
	 */
	public AmanuensisManager(EmbeddedCacheManager cacheManager) {
		if (cacheManager==null) {
			throw new IllegalArgumentException("cacheManager cannot be null");
		}
		if (cacheManager.getStatus()!=ComponentStatus.RUNNING) {
			throw new IllegalStateException("Cache is not running");
		}
		this.cacheManager = cacheManager;
		this.writerMap = new Memoizer<String, AmanuensisIndexWriter>(new IndexWriterMemoizer());
		this.readerMap = new Memoizer<String, AmanuensisIndexReader>(new IndexReaderMemoizer());
		this.remoteOperationProcessor = new JGroupsOperationReceiver(this, INFINISPAN_INDEX_WRITER_SCOPE_ID);
		this.remoteOperationDispatcher = new JGroupsOperationDispatcher(this, this.remoteOperationProcessor.getDispatcher());
		this.localOperationDispatcher = new LuceneOperationDispatcher(this);
	}
	
	/**
	 * Initializes writers and readers for all the directories
	 * 
	 * @param directories
	 * @throws IndexerException
	 */
	public void initialize(Directory...directories) throws IndexerException {
		for(Directory directory : directories) {
			getIndexWriter(directory);
			getIndexReader(directory);
		}
	}

	public WriterConfigurator getWriterConfigurator() {
		return writerConfigurator;
	}

	public void setWriterConfigurator(WriterConfigurator writerConfigurator) {
		this.writerConfigurator = writerConfigurator;
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
		if (analyzer==null) {
			throw new IllegalArgumentException("analyzer cannot be null");
		}
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
	public AmanuensisIndexWriter getIndexWriter(Directory directory) throws IndexerException {
		if (directory==null) {
			throw new IllegalArgumentException("directory cannot be null");
		}
		String directoryId = getUniqueDirectoryIdentifier(directory);
		try {			
			directoryMap.putIfAbsent(directoryId, directory);
			return writerMap.compute(directoryId);
		} catch (Exception e) {
			log.error("Could not obtain an IndexWriter");
			throw new IndexerException(e);
		}
	}
	
	public AmanuensisIndexWriter getIndexWriter(String indexName) throws IndexerException {
		if (indexName==null) {
			throw new IllegalArgumentException("directory cannot be null");
		}
		if(!directoryMap.containsKey(indexName))
			throw new IndexerException("Unknown index "+indexName);
		try {
			
			return writerMap.compute(indexName);
		} catch (Exception e) {
			log.error("Could not obtain an IndexWriter for "+indexName);
			throw new IndexerException(e);
		}
	}
	
	
	/**
	 * Retrieves (or initializes) an instance of AmanuensisIndexReader for the specified
	 * directory
	 * 
	 * @param directory
	 * @return
	 * @throws IndexerException
	 */
	public AmanuensisIndexReader getIndexReader(Directory directory) throws IndexerException {
		if (directory==null) {
			throw new IllegalArgumentException("directory cannot be null");
		}
		String directoryId = getUniqueDirectoryIdentifier(directory);
		try {
			directoryMap.putIfAbsent(directoryId, directory);
			return readerMap.compute(directoryId);
		} catch (Exception e) {
			log.error("Could not obtain an IndexReader");
			throw new IndexerException(e);
		}
	}
	
	public AmanuensisIndexReader getIndexReader(String indexName) throws IndexerException {
		if (indexName==null) {
			throw new IllegalArgumentException("directory cannot be null");
		}
		if(!directoryMap.containsKey(indexName))
			throw new IndexerException("Unknown index "+indexName);
		try {
			
			return readerMap.compute(indexName);
		} catch (Exception e) {
			log.error("Could not obtain an IndexReader for "+indexName);
			throw new IndexerException(e);
		}
	}
	
	public static String getUniqueDirectoryIdentifier(Directory directory) {
		if(directory instanceof FSDirectory) {
			return ((FSDirectory)directory).getFile().getAbsolutePath();
		} else if (directory instanceof InfinispanDirectory) {
			return ((InfinispanDirectory)directory).getIndexName();
		} else {
			throw new RuntimeException("Unknown Directory implementation = "+directory.getClass().getName());
		}
		
	}

	/**
	 * Returns the {@link Directory} for the specified indexName
	 * 
	 * @param indexName
	 * @return
	 */
	public Directory getDirectoryByIndexName(String indexName) {
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

	private class IndexWriterMemoizer implements Computable<String, AmanuensisIndexWriter> {
		@Override
		public AmanuensisIndexWriter compute(String indexName) throws InterruptedException, ExecutionException {
			try {
				return new AmanuensisIndexWriter(AmanuensisManager.this, AmanuensisManager.this.directoryMap.get(indexName));
			} catch (IndexerException e) {
				throw new ExecutionException(e);
			}
		}

	}
	
	private class IndexReaderMemoizer implements Computable<String, AmanuensisIndexReader> {
		@Override
		public AmanuensisIndexReader compute(String indexName) throws InterruptedException, ExecutionException {
			try {
				return new AmanuensisIndexReader(AmanuensisManager.this, AmanuensisManager.this.directoryMap.get(indexName));
			} catch (IndexerException e) {
				throw new ExecutionException(e);
			}
		}

	}

}
