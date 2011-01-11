package net.dataforte.infinispan.amanuensis.ops;

import net.dataforte.infinispan.amanuensis.IndexOperation;

import org.apache.lucene.document.Document;

public class AddDocumentOperation extends IndexOperation {
	Document doc;

	public AddDocumentOperation(Document doc) {		
		this.doc = doc;
	}

	public Document getDoc() {
		return doc;
	}

	public void setDoc(Document doc) {
		this.doc = doc;
	}
}
