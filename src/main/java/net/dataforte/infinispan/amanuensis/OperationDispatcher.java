package net.dataforte.infinispan.amanuensis;

public interface OperationDispatcher {
	void dispatch(IndexOperations ops) throws IndexerException;
}
