package net.dataforte.infinispan.amanuensis.backend.infinispan;

import java.util.concurrent.ExecutionException;

import net.dataforte.commons.collections.Computable;
import net.dataforte.commons.collections.Memoizer;
import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.AmanuensisIndexWriter;
import net.dataforte.infinispan.amanuensis.AmanuensisManager;
import net.dataforte.infinispan.amanuensis.IndexOperations;
import net.dataforte.infinispan.amanuensis.IndexerException;
import net.dataforte.infinispan.amanuensis.OperationDispatcher;
import net.dataforte.infinispan.amanuensis.backend.jgroups.JGroupsOperationDispatcher;
import net.dataforte.infinispan.amanuensis.backend.jgroups.JGroupsOperationProcessor;
import net.dataforte.infinispan.amanuensis.backend.lucene.LuceneOperationDispatcher;

import org.apache.lucene.store.Directory;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.slf4j.Logger;

public class AmanuensisInfinispanManager extends AmanuensisManager {
	private static final Logger log = LoggerFactory.make();
	private static final short INFINISPAN_INDEX_WRITER_SCOPE_ID = 1234;
	private final EmbeddedCacheManager cacheManager;
	private JGroupsOperationProcessor remoteOperationProcessor;
	private OperationDispatcher remoteOperationDispatcher;
	private OperationDispatcher localOperationDispatcher;
	private Memoizer<String, AmanuensisIndexWriter> writerMap;

	/**
	 * Constructs an {@link AmanuensisManager} using the specified
	 * {@link EmbeddedCacheManager}
	 * 
	 * @param cacheManager
	 */
	public AmanuensisInfinispanManager(EmbeddedCacheManager cacheManager) {
		if (cacheManager == null) {
			throw new IllegalArgumentException("cacheManager cannot be null");
		}
		if (cacheManager.getStatus() != ComponentStatus.RUNNING) {
			throw new IllegalStateException("Cache is not running");
		}
		this.cacheManager = cacheManager;
		this.writerMap = new Memoizer<String, AmanuensisIndexWriter>(new InfinispanIndexWriterMemoizer());
		this.remoteOperationProcessor = new JGroupsOperationProcessor(this, INFINISPAN_INDEX_WRITER_SCOPE_ID);
		this.remoteOperationDispatcher = new JGroupsOperationDispatcher(this, this.remoteOperationProcessor.getDispatcher());
		this.localOperationDispatcher = new LuceneOperationDispatcher(this);
	}

	/**
	 * Retrieves (or initializes) an instance of InfinispanIndexWriter for the
	 * specified directory
	 * 
	 * @param directory
	 * @return
	 */
	public AmanuensisIndexWriter getIndexWriter(Directory directory) throws IndexerException {
		if (directory == null) {
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

	private class InfinispanIndexWriterMemoizer implements Computable<String, AmanuensisIndexWriter> {
		@Override
		public AmanuensisIndexWriter compute(String indexName) throws InterruptedException, ExecutionException {
			try {
				return new AmanuensisIndexWriter(AmanuensisInfinispanManager.this, AmanuensisInfinispanManager.this.directoryMap.get(indexName));
			} catch (IndexerException e) {
				throw new ExecutionException(e);
			}
		}

	}

	public void close() {
		this.remoteOperationProcessor.close();
	}

	/**
	 * Returns the cluster's {@link Address} of the master
	 * 
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

	@Override
	public void dispatchOperations(IndexOperations indexOperations) throws IndexerException {
		if (cacheManager.isCoordinator()) {
			// process the messages locally
			this.localOperationDispatcher.dispatch(indexOperations);
		} else {
			// send them to the remote
			this.remoteOperationDispatcher.dispatch(indexOperations);
		}
	}

}
