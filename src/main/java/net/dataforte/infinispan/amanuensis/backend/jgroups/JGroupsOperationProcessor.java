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

package net.dataforte.infinispan.amanuensis.backend.jgroups;

import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.AmanuensisManager;
import net.dataforte.infinispan.amanuensis.IndexOperations;
import net.dataforte.infinispan.amanuensis.IndexerException;

import org.apache.lucene.index.IndexWriter;
import org.jgroups.Message;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.mux.MuxMessageDispatcher;
import org.slf4j.Logger;

/**
 * This class receives incoming messages from neighboring nodes, and passes them
 * onto an executor, which will then apply them to the appropriate
 * {@link IndexWriter}
 * 
 * @author Tristan Tarrant
 */
public class JGroupsOperationProcessor implements RequestHandler {
	private static final Logger log = LoggerFactory.make();
	private AmanuensisManager manager;
	private MuxMessageDispatcher dispatcher;

	public JGroupsOperationProcessor(AmanuensisManager manager, short scopeId) {
		this.manager = manager;
		// Register the dispatcher for receiving messages
		this.dispatcher = new MuxMessageDispatcher(scopeId, MuxChannelLookup.getChannel(), null, null, this);
	}

	public void close() {
		this.dispatcher.stop();
	}

	public MuxMessageDispatcher getDispatcher() {
		return dispatcher;
	}

	@Override
	public Object handle(Message msg) {
		IndexOperations ops;

		try {
			ops = (IndexOperations) msg.getObject();
		} catch (ClassCastException e) {
			log.error("Unexpected message received", e);
			return null;
		}

		if (ops != null) {
			if (log.isDebugEnabled()) {
				log.debug("Received {} operations from {}\n", ops, msg.getSrc());
			}
			// The manager knows what to do
			try {
				manager.dispatchOperations(ops);
			} catch (IndexerException e) {
				log.error("Error while dispatching operations {} received from {}", msg.getSrc(), ops);
			}
		}
		return null;
	}
}
