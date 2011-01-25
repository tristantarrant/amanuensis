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

package net.dataforte.infinispan.amanuensis.backend.lucene;

import java.io.IOException;

import net.dataforte.infinispan.amanuensis.IndexOperation;

import org.apache.lucene.index.IndexWriter;


public abstract class OperationExecutor<T extends IndexOperation> {

	 public abstract void execute(IndexWriter w, T op) throws IOException;
	
	@SuppressWarnings(value="unchecked")
	public void exec(IndexWriter w, IndexOperation op) throws IOException {
		this.execute(w, (T)op);
	}
	
}
