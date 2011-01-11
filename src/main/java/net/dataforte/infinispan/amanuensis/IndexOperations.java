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
