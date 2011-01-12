/**
 * Copyright 2010 Tristan Tarrant
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dataforte.infinispan.amanuensis.backend.jgroups;

import net.dataforte.commons.slf4j.LoggerFactory;
import net.dataforte.infinispan.amanuensis.AmanuensisManager;
import net.dataforte.infinispan.amanuensis.IndexOperations;
import net.dataforte.infinispan.amanuensis.IndexerException;
import net.dataforte.infinispan.amanuensis.OperationDispatcher;

import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.SuspectedException;
import org.jgroups.TimeoutException;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.mux.MuxMessageDispatcher;
import org.slf4j.Logger;

/**
 * This class takes care of dispatching {@link IndexOperations} messages
 * from the slaves to the master.
 * 
 * @author Tristan Tarrant
 */
public class JGroupsOperationDispatcher implements OperationDispatcher {
	private static final Logger log = LoggerFactory.make();
	private AmanuensisManager manager;
	private MuxMessageDispatcher messageDispatcher;

	public JGroupsOperationDispatcher(AmanuensisManager manager, MuxMessageDispatcher messageDispatcher) {
		this.manager = manager;
		this.messageDispatcher = messageDispatcher;
	}

	@Override
	public void dispatch(IndexOperations ops) throws IndexerException {
		Address dest = ((JGroupsAddress)manager.getMasterAddress()).getJGroupsAddress();
		Address src = ((JGroupsAddress)manager.getLocalAddress()).getJGroupsAddress();
		Message message = new Message(dest, src, ops);		
		try {
			if(log.isDebugEnabled()) {
				log.debug("Sending {} to {}", ops.toString(), dest.toString());
			}
			messageDispatcher.sendMessage(message, RequestOptions.ASYNC);
		} catch (SuspectedException e) {
			throw new IndexerException(e);
		} catch (TimeoutException e) {
			throw new IndexerException(e);
		}
	}

}
