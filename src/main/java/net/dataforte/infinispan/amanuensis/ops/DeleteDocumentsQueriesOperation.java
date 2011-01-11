package net.dataforte.infinispan.amanuensis.ops;

import net.dataforte.infinispan.amanuensis.IndexOperation;

import org.apache.lucene.search.Query;

public class DeleteDocumentsQueriesOperation extends IndexOperation {
	Query queries[];

	public DeleteDocumentsQueriesOperation(Query... queries) {		
		this.queries = queries;
	}

	public Query[] getQueries() {
		return queries;
	}

	public void setQueries(Query[] queries) {
		this.queries = queries;
	}

}
