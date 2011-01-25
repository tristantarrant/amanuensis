package net.dataforte.infinispan.amanuensis.backend.elasticsearch;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.slf4j.Logger;

import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.IndexOperation;
import net.dataforte.infinispan.amanuensis.IndexOperations;
import net.dataforte.infinispan.amanuensis.backend.lucene.LuceneExecutorContext;

public class ElasticSearchOperationQueueExecutor implements Runnable {

	private static final Logger log = LoggerFactory.make();

	private ElasticSearchExecutorContext context;
	private IndexOperations ops;

	public ElasticSearchOperationQueueExecutor(ElasticSearchExecutorContext context, IndexOperations ops) {
		this.context = context;		
		this.ops = ops;
	}
	

	@Override
	public void run() {
		if (this.ops.getOperations().isEmpty()) {
			return;
		}
		try {
			// Obtain an index writer
			Client client = context.getClient();
			BulkRequestBuilder bulk = client.prepareBulk();			
			for (IndexOperation op : ops.getOperations()) {
				
				Class<? extends IndexOperation> opClass = op.getClass();								
				OperationExecutor<? extends IndexOperation> executor = context.getOperationExecutorFactory().getExecutor(opClass);		
				executor.exec(ops.getIndexName(), client, bulk, op);
			}
			// Commit the changes
			bulk.execute().actionGet();
		} catch (Throwable t) {
			log.error("Error while processing queue for index "+ops.getIndexName(), t);
		}

	}

}
