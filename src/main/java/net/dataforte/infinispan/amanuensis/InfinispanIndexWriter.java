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

import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.ops.AddDocumentOperation;
import net.dataforte.infinispan.amanuensis.ops.DeleteDocumentsQueriesOperation;
import net.dataforte.infinispan.amanuensis.ops.DeleteDocumentsTermsOperation;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.infinispan.lucene.InfinispanDirectory;
import org.slf4j.Logger;

public class InfinispanIndexWriter {
	private static final Logger log = LoggerFactory.make();

	private AmanuensisManager manager;
	private String indexName;
	private ThreadLocal<IndexOperations> batchOps;

	private InfinispanDirectory directory;

	public InfinispanIndexWriter(AmanuensisManager manager, InfinispanDirectory directory) {
		this.manager = manager;
		this.indexName = directory.getIndexName();
		this.directory = directory;
		this.batchOps = new ThreadLocal<IndexOperations>();
	}

	public String getIndexName() {
		return indexName;
	}

	public InfinispanDirectory getDirectory() {
		return directory;
	}

	// IndexWriter methods

	public boolean isBatching() {
		return batchOps.get() != null;
	}

	/**
	 * Put this InfinispanIndexWriter in batch mode: all operations will be
	 * queued and sent when the {@link InfinispanIndexWriter#endBatch} method is
	 * invoked. Note that the batches are local to the current thread, therefore
	 * each thread may start and end a batch indipendently from the others.
	 * 
	 * @see InfinispanIndexWriter#endBatch()
	 * @see InfinispanIndexWriter#cancelBatch()
	 */
	public void startBatch() {
		if (isBatching()) {
			throw new IllegalStateException("Already in batching mode");
		} else {
			batchOps.set(new IndexOperations(this.indexName));

			if (log.isDebugEnabled()) {
				log.debug("Batching started for index " + indexName);
			}
		}
	}

	/**
	 * Send all changes in the current batch, started by
	 * {@link InfinispanIndexWriter#startBatch()} to the master node for
	 * indexing
	 * 
	 * @throws IndexerException
	 * 
	 * @see InfinispanIndexWriter#startBatch()
	 * @see InfinispanIndexWriter#cancelBatch()
	 */
	public void endBatch() throws IndexerException {
		if (!isBatching()) {
			throw new IllegalStateException("Not in batching mode");
		} else {
			manager.dispatchOperations(batchOps.get());
			batchOps.remove();
			if (log.isDebugEnabled()) {
				log.debug("Batching finished for index " + indexName);
			}
		}
	}

	/**
	 * Cancels the current batch: all queued changes will be reset and not sent
	 * to the master
	 * 
	 * @see InfinispanIndexWriter#startBatch()
	 * @see InfinispanIndexWriter#endBatch()
	 */
	public void cancelBatch() {
		if (!isBatching()) {
			throw new IllegalStateException("Not in batching mode");
		} else {
			batchOps.remove();
			if (log.isDebugEnabled()) {
				log.debug("Batching cancelled for index " + indexName);
			}
		}
	}

	/**
	 * Adds a single {@link Document} to the index
	 * 
	 * @param doc
	 * @throws IndexerException
	 */
	public void addDocument(Document doc) throws IndexerException {
		dispatch(new AddDocumentOperation(doc));
	}

	/**
	 * Adds multiple {@link Document} to the index
	 * 
	 * @param docs
	 * @throws IndexerException
	 */
	public void addDocuments(Document... docs) throws IndexerException {
		if (docs.length == 0) {
			return;
		}
		IndexOperation ops[] = new IndexOperation[docs.length];
		for (int i = 0; i < ops.length; i++) {
			ops[i] = new AddDocumentOperation(docs[i]);
		}
		dispatch(ops);
	}

	/**
	 * Deletes all documents from the index which match the given array of
	 * {@link Query}
	 * 
	 * @param queries
	 * @throws IndexerException
	 */
	public void deleteDocuments(Query... queries) throws IndexerException {
		if (queries.length == 0) {
			return;
		}
		IndexOperation ops[] = new IndexOperation[queries.length];
		for (int i = 0; i < queries.length; i++) {
			ops[i] = new DeleteDocumentsQueriesOperation(queries[i]);
		}
		dispatch(ops);
	}

	/**
	 * Deletes all documents from the index which match the given array of
	 * {@link Term}
	 * 
	 * @param queries
	 * @throws IndexerException
	 */
	public void deleteDocuments(Term... terms) throws IndexerException {
		if (terms.length == 0) {
			return;
		}
		IndexOperation ops[] = new IndexOperation[terms.length];
		for (int i = 0; i < terms.length; i++) {
			ops[i] = new DeleteDocumentsTermsOperation(terms[i]);
		}
		dispatch(ops);
	}

	// INTERNAL METHODS
	private void dispatch(IndexOperation... ops) throws IndexerException {
		if (isBatching()) {
			batchOps.get().addOperations(ops);
		} else {
			manager.dispatchOperations(new IndexOperations(this.indexName, ops));
		}
	}

}
