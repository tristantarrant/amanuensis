package net.dataforte.infinispan.amanuensis;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

public class AmanuensisIndexReader {
	IndexReader r;

	public AmanuensisIndexReader(AmanuensisManager amanuensisManager, Directory directory) throws IndexerException {
		try {
			r = IndexReader.open(directory, true);
		} catch (Exception e) {
			throw new IndexerException("Could not create IndexReader", e);
		}
	}

	public synchronized IndexReader getReader() throws IndexerException {
		try {
			IndexReader freshReader = r.reopen();
			
			return freshReader;
		} catch (Exception e) {
			throw new IndexerException("Could not refresh IndexReader", e);
		}
	}

}
