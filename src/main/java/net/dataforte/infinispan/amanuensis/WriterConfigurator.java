package net.dataforte.infinispan.amanuensis;

import org.apache.lucene.index.IndexWriter;

public interface WriterConfigurator {
	public void configure(IndexWriter w);
}
