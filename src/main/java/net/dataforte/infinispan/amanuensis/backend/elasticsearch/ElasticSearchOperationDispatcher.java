package net.dataforte.infinispan.amanuensis.backend.elasticsearch;

import java.util.concurrent.ExecutionException;

import net.dataforte.commons.collections.Computable;
import net.dataforte.commons.collections.Memoizer;
import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.AmanuensisManager;
import net.dataforte.infinispan.amanuensis.IndexOperations;
import net.dataforte.infinispan.amanuensis.IndexerException;
import net.dataforte.infinispan.amanuensis.OperationDispatcher;
import net.dataforte.infinispan.amanuensis.backend.lucene.LuceneOperationQueueExecutor;
import net.dataforte.infinispan.amanuensis.backend.lucene.LuceneExecutorContext;



import org.slf4j.Logger;

public class ElasticSearchOperationDispatcher implements OperationDispatcher {
	private static final Logger log = LoggerFactory.make();
	private AmanuensisManager manager;
	private Memoizer<String, ElasticSearchExecutorContext> executorContexts;

	public ElasticSearchOperationDispatcher(AmanuensisManager amanuensisManager) {
		this.manager = amanuensisManager;
	}
	
	@Override
	public void dispatch(IndexOperations ops) throws IndexerException {
		try {
			ElasticSearchExecutorContext context = executorContexts.compute(ops.getIndexName());
			ElasticSearchOperationQueueExecutor queueExecutor = new ElasticSearchOperationQueueExecutor(context, ops);
			context.getExecutor().execute(queueExecutor);
		} catch (Exception e) {
			log.error("", e);
		}
	}
	
	private class ExecutorContextComputer implements Computable<String, ElasticSearchExecutorContext> {
		@Override
		public ElasticSearchExecutorContext compute(String indexName) throws InterruptedException, ExecutionException {
			ElasticSearchExecutorContext executorContext = new ElasticSearchExecutorContext(indexName);
			return executorContext;			
		}
		
	}

}
