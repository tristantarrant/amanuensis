/**
 * Amanuensis, a distributed Lucene Index Writer for Infinispan
 *
 * Copyright (c) 2011, Tristan Tarrant
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */

package net.dataforte.infinispan.amanuensis.backend.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import net.dataforte.infinispan.amanuensis.ops.AddDocumentOperation;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;


public class AddDocumentExecutor extends OperationExecutor<AddDocumentOperation> {

	@Override
	public void execute(String indexName, Client c, BulkRequestBuilder b, AddDocumentOperation op) throws IOException {
		IndexRequest request = new IndexRequest(indexName).type(indexName);
		XContentBuilder json = jsonBuilder().startObject();
		
		Document doc = op.getDoc();
		for(Fieldable field : doc.getFields()) {
			if(field instanceof Field) {
				json.field(field.name(), field.stringValue());
			} else {
				throw new IllegalArgumentException("FIXME: Can't handle fields of type "+field.getClass().getName());
			}
			
		}
		request.source(json);
		b.add(request);
	}

}
