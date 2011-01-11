package net.dataforte.infinispan.amanuensis.backend.lucene;

import java.io.IOException;

import net.dataforte.infinispan.amanuensis.OperationExecutor;
import net.dataforte.infinispan.amanuensis.ops.DeleteDocumentsTermsOperation;

import org.apache.lucene.index.IndexWriter;

public class DeleteDocumentsTermsExecutor extends OperationExecutor<DeleteDocumentsTermsOperation> {

	@Override
	public void execute(IndexWriter w, DeleteDocumentsTermsOperation op) throws IOException {		
		w.deleteDocuments(op.getTerms());
	}


}
