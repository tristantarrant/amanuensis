/**
 * Copyright 2010 Tristan Tarrant
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
