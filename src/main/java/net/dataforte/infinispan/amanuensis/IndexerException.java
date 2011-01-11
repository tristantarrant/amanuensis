package net.dataforte.infinispan.amanuensis;



public class IndexerException extends Exception {

	public IndexerException(Throwable t) {
		super(t);
	}

	public IndexerException(String msg) {
		super(msg);
	}

	public IndexerException(String msg, Throwable t) {
		super(msg, t);
	}

}
