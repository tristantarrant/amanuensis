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
	private boolean batching;
	private IndexOperations batchOps;

	private InfinispanDirectory directory;

	public InfinispanIndexWriter(AmanuensisManager manager, InfinispanDirectory directory) {
		this.manager = manager;
		this.indexName = directory.getIndexName();
		this.directory = directory;
	}

	public String getIndexName() {
		return indexName;
	}
	
	public InfinispanDirectory getDirectory() {
		return directory;
	}

	// IndexWriter methods

	public synchronized void startBatch() {
		if (batching) {
			throw new IllegalStateException("Already in batching mode");
		} else {
			batchOps = new IndexOperations(this.indexName);
			batching = true;
			if(log.isDebugEnabled()) {
				log.debug("Batching started for index "+indexName);
			}
		}
	}

	public synchronized void endBatch() throws IndexerException {
		if (!batching) {
			throw new IllegalStateException("Not in batching mode");
		} else {
			batching = false;
			manager.dispatchOperations(batchOps);
			batchOps = null;
			if(log.isDebugEnabled()) {
				log.debug("Batching finished for index "+indexName);
			}
		}
	}

	public void addDocument(Document doc) throws IndexerException {
		dispatch(new AddDocumentOperation(doc));
	}

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
		if (batching) {
			batchOps.addOperations(ops);
		} else {
			manager.dispatchOperations(new IndexOperations(this.indexName, ops));
		}
	}

}
