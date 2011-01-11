package net.dataforte.infinispan.amanuensis.backend.lucene;

import java.io.IOException;

import net.dataforte.infinispan.amanuensis.OperationExecutor;
import net.dataforte.infinispan.amanuensis.ops.AddDocumentOperation;

import org.apache.lucene.index.IndexWriter;

public class AddDocumentExecutor extends OperationExecutor<AddDocumentOperation> {
	
	@Override
	public void execute(IndexWriter w, AddDocumentOperation op) throws IOException {		
		w.addDocument(op.getDoc());
	}

}
