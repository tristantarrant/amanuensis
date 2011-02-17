/**
 * Amanuensis, a distributed Lucene Index Writer for Infinispan
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

package net.dataforte.infinispan.amanuensis.backend.lucene;

import java.util.concurrent.ExecutionException;

import net.dataforte.commons.collections.Computable;
import net.dataforte.commons.collections.Memoizer;
import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.AmanuensisManager;
import net.dataforte.infinispan.amanuensis.ExecutorContext;
import net.dataforte.infinispan.amanuensis.IndexOperations;
import net.dataforte.infinispan.amanuensis.IndexerException;
import net.dataforte.infinispan.amanuensis.OperationDispatcher;

import org.slf4j.Logger;

public class LuceneOperationDispatcher implements OperationDispatcher {
	private static final Logger log = LoggerFactory.make();
	private AmanuensisManager manager;
	private Memoizer<String, ExecutorContext> executorContexts;

	public LuceneOperationDispatcher(AmanuensisManager manager) {
		this.manager = manager;		
		this.executorContexts = new Memoizer<String, ExecutorContext>(new ExecutorContextComputer());
	}
	
	public void checkIndex(String indexName, boolean fix) {
		try {
			ExecutorContext context = executorContexts.compute(indexName);
			context.check(fix);
		} catch (Exception e) {
			log.error("", e);
		}
	}

	@Override
	public void dispatch(IndexOperations ops) throws IndexerException {
		try {
			ExecutorContext context = executorContexts.compute(ops.getIndexName());
			DirectoryOperationQueueExecutor queueExecutor = new DirectoryOperationQueueExecutor(context, ops);
			context.getExecutor().execute(queueExecutor);
		} catch (Exception e) {
			log.error("", e);
		}
	}
	
	private class ExecutorContextComputer implements Computable<String, ExecutorContext> {
		@Override
		public ExecutorContext compute(String indexName) throws InterruptedException, ExecutionException {
			ExecutorContext executorContext = new ExecutorContext(manager, LuceneOperationDispatcher.this.manager.getDirectoryByIndexName(indexName), LuceneOperationDispatcher.this.manager.getAnalyzer());
			return executorContext;			
		}
		
	}

}
