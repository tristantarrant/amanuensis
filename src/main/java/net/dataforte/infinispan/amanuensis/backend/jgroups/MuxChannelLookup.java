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

import java.net.URL;
import java.util.Properties;

import org.infinispan.CacheException;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelLookup;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.FileLookup;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MuxChannelLookup implements JGroupsChannelLookup {	
	private static final Logger log = LoggerFactory.getLogger(MuxChannelLookup.class);
	private static MuxChannel muxedChannel;

	@Override
	public Channel getJGroupsChannel(Properties p) {

		String cfg = p.getProperty(JGroupsTransport.CONFIGURATION_FILE);
		try {
			URL cfgUrl = new FileLookup().lookupFileLocation(cfg);
			JChannel channel = new JChannel(cfgUrl);
			muxedChannel = new MuxChannel(channel);
			log.info("MuxChannel created");
			return muxedChannel;
		} catch (Exception e) {
			log.error("Error while trying to create a channel using config files: " + cfg);
			throw new CacheException(e);
		}
	}

	public static MuxChannel getChannel() {
		return muxedChannel;
	}

	@Override
	public boolean shouldStartAndConnect() {
		return true;
	}

	@Override
	public boolean shouldStopAndDisconnect() {
		return true;
	}	

}
