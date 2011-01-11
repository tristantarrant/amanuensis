package net.dataforte.infinispan.amanuensis.ops;

import net.dataforte.infinispan.amanuensis.IndexOperation;

import org.apache.lucene.index.Term;

public class DeleteDocumentsTermsOperation extends IndexOperation {
	Term terms[];

	public DeleteDocumentsTermsOperation(Term... terms) {
		this.terms = terms;
	}

	public Term[] getTerms() {
		return terms;
	}

	public void setTerms(Term[] terms) {
		this.terms = terms;
	}

}
