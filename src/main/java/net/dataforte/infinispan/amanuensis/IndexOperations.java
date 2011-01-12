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

package net.dataforte.infinispan.amanuensis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements the message that is sent 
 * from the slaves to the master. It contains the name
 * of the index on which to operate and an ordered list of
 * operations to apply to that index
 * 
 * @author Tristan Tarrant
 */
public class IndexOperations implements Serializable {
	final String indexName;
	List<IndexOperation> operations = new ArrayList<IndexOperation>();

	public IndexOperations(String indexName) {
		this.indexName = indexName;
	}
	
	public IndexOperations(String indexName, List<IndexOperation> operations) {
		this.indexName = indexName;
		addOperations(operations);
	}
	
	public IndexOperations(String indexName, IndexOperation... operations) {
		this.indexName = indexName;
		addOperations(operations);
	}
	
	public void addOperations(List<IndexOperation> operations) {
		this.operations.addAll(operations);
	}

	public void addOperations(IndexOperation... ops) {
		for (IndexOperation op : ops) {
			operations.add(op);
		}
	}

	public String getIndexName() {
		return indexName;
	}

	public List<IndexOperation> getOperations() {
		return operations;
	}

	@Override
	public String toString() {
		return "IndexOperations [indexName=" + indexName + ", operations=" + operations + "]";
	}
}
