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

package net.dataforte.infinispan.amanuensis.dummy;

import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.AmanuensisManager;
import net.dataforte.infinispan.amanuensis.IndexOperations;
import net.dataforte.infinispan.amanuensis.IndexerException;
import net.dataforte.infinispan.amanuensis.OperationDispatcher;

import org.jgroups.blocks.mux.MuxMessageDispatcher;
import org.slf4j.Logger;

/**
 * This class takes care of dispatching {@link IndexOperations} messages from
 * the slaves to the master.
 * 
 * @author Tristan Tarrant
 */
public class DummyOperationDispatcher implements OperationDispatcher {
	private static final Logger log = LoggerFactory.make();
	private AmanuensisManager manager;


	public DummyOperationDispatcher(AmanuensisManager manager, MuxMessageDispatcher messageDispatcher) {
		this.manager = manager;
		
	}



	@Override
	public void dispatch(final IndexOperations ops) throws IndexerException {
		if(log.isDebugEnabled()) {
			log.debug("Discarding operations {}", ops);
		}
	}
}
