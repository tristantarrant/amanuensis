package net.dataforte.infinispan.amanuensis.backend.lucene;

import java.io.IOException;

import net.dataforte.infinispan.amanuensis.OperationExecutor;
import net.dataforte.infinispan.amanuensis.ops.OptimizeIndexOperation;

import org.apache.lucene.index.IndexWriter;

public class OptimizeIndexExecutor extends OperationExecutor<OptimizeIndexOperation> {

	@Override
	public void execute(IndexWriter w, OptimizeIndexOperation op) throws IOException {
		w.optimize();
	}

}
