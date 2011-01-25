package net.dataforte.infinispan.amanuensis.backend.elasticsearch;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * This class is just a placeholder, it's supposed to do nothing
 * 
 * @author Tristan Tarrant
 *
 */
public class ElasticSearchDirectory extends Directory {

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public IndexOutput createOutput(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteFile(String arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean fileExists(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long fileLength(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long fileModified(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String[] listAll() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IndexInput openInput(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void touchFile(String arg0) throws IOException {
		// TODO Auto-generated method stub

	}

}
