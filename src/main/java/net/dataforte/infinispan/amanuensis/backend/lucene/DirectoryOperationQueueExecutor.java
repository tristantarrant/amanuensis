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

import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.ExecutorContext;
import net.dataforte.infinispan.amanuensis.IndexOperation;
import net.dataforte.infinispan.amanuensis.IndexOperations;
import net.dataforte.infinispan.amanuensis.OperationExecutor;

import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;

/**
 * 
 * @author Tristan Tarrant
 */
public class DirectoryOperationQueueExecutor implements Runnable {
	private static final Logger log = LoggerFactory.make();

	private ExecutorContext context;
	private IndexOperations ops;

	public DirectoryOperationQueueExecutor(ExecutorContext context, IndexOperations ops) {
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
			IndexWriter writer = context.getWriter();
			for (IndexOperation op : ops.getOperations()) {
				Class<? extends IndexOperation> opClass = op.getClass();

				OperationExecutor<? extends IndexOperation> executor = context.getOperationExecutorFactory().getExecutor(opClass);
				executor.exec(writer, op);
			}
			// Commit the changes
			context.commit();
		} catch (Throwable t) {
			log.error("Error while processing queue for index "+ops.getIndexName()+", discarding writer and unlocking directory", t);
			context.forceUnlock();
			
			// FIXME, we should retry the operation
		}

	}

}
