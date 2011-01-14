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
