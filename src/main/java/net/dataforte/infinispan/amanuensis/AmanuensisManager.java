/**
 * Amanuensis, a distributed Lucene Index Writer
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * AmanuensisManager is the entry point for obtaining {@link AmanuensisIndexWriter} instances.
 * It handles synchronization between the nodes and matching between the writers and their
 * respective {@link InfinispanDirectory} instances. There should be only one instance of
 * AmanuensisManager for each {@link EmbeddedCacheManager}.
 * 
 * @author Tristan Tarrant
 *
 */
public abstract class AmanuensisManager {
	
	private static final Analyzer SIMPLE_ANALYZER = new SimpleAnalyzer();
	
	protected ConcurrentMap<String, Directory> directoryMap = new ConcurrentHashMap<String, Directory>();
	
	private Analyzer analyzer = SIMPLE_ANALYZER;

	

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
	public abstract void close();

	
	
	public AmanuensisIndexReader getIndexReader(String name) throws IndexerException {
		return null;
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
	 * Dispatches the message to the appropriate destination depending on the
	 * role of this node.
	 * 
	 * @param indexOperations
	 * @throws IndexerException
	 */
	public abstract void dispatchOperations(IndexOperations indexOperations) throws IndexerException;

}
