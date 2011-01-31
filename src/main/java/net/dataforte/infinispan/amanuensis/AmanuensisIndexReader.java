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
			
			if(freshReader!=r) {	// If the freshReader is really "fresh", we should close the previous one
				r.close(); //FIXME: should only close when there are no other users left
			}
			
			return freshReader;
		} catch (Exception e) {
			throw new IndexerException("Could not refresh IndexReader", e);
		}
	}

}
