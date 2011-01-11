package net.dataforte.infinispan.amanuensis.backend.lucene;

import java.util.HashMap;
import java.util.Map;

import net.dataforte.infinispan.amanuensis.IndexOperation;
import net.dataforte.infinispan.amanuensis.OperationExecutor;
import net.dataforte.infinispan.amanuensis.ops.AddDocumentOperation;
import net.dataforte.infinispan.amanuensis.ops.DeleteDocumentsQueriesOperation;
import net.dataforte.infinispan.amanuensis.ops.DeleteDocumentsTermsOperation;
import net.dataforte.infinispan.amanuensis.ops.OptimizeIndexOperation;

public class LuceneOperationExecutorFactory {
	Map<Class<? extends IndexOperation>, OperationExecutor<? extends IndexOperation>> executor = new HashMap<Class<? extends IndexOperation>, OperationExecutor<? extends IndexOperation>>();
	
	public LuceneOperationExecutorFactory() {
		executor.put(AddDocumentOperation.class, new AddDocumentExecutor());
		executor.put(DeleteDocumentsTermsOperation.class, new DeleteDocumentsTermsExecutor());
		executor.put(DeleteDocumentsQueriesOperation.class, new DeleteDocumentsQueriesExecutor());
		executor.put(OptimizeIndexOperation.class, new OptimizeIndexExecutor());
	}

	public OperationExecutor<? extends IndexOperation> getExecutor(Class<? extends IndexOperation> klass) {
		return executor.get(klass);
	}
}
