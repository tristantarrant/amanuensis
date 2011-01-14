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
