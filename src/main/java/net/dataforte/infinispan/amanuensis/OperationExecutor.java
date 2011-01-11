package net.dataforte.infinispan.amanuensis;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;


public abstract class OperationExecutor<T extends IndexOperation> {

	 public abstract void execute(IndexWriter w, T op) throws IOException;
	
	@SuppressWarnings(value="unchecked")
	public void exec(IndexWriter w, IndexOperation op) throws IOException {
		this.execute(w, (T)op);
	}
	
}
