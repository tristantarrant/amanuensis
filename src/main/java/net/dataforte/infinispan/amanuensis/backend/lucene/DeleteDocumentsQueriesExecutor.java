package net.dataforte.infinispan.amanuensis.backend.lucene;

import java.io.IOException;

import net.dataforte.infinispan.amanuensis.OperationExecutor;
import net.dataforte.infinispan.amanuensis.ops.DeleteDocumentsQueriesOperation;

import org.apache.lucene.index.IndexWriter;

public class DeleteDocumentsQueriesExecutor extends OperationExecutor<DeleteDocumentsQueriesOperation> {

	@Override
	public void execute(IndexWriter w, DeleteDocumentsQueriesOperation op) throws IOException {
		w.deleteDocuments(op.getQueries());		
	}

}
