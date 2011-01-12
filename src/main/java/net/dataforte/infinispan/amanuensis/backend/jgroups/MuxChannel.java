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

import java.io.File;
import java.net.URL;

import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.UpHandler;
import org.jgroups.blocks.mux.MuxUpHandler;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.w3c.dom.Element;

public class MuxChannel extends JChannel {
	public MuxChannel() throws ChannelException {
		super();
		init();
	}

	public MuxChannel(Element properties) throws ChannelException {
		super(properties);
		init();
	}

	public MuxChannel(File properties) throws ChannelException {
		super(properties);
		init();
	}

	public MuxChannel(JChannel ch) throws ChannelException {
		super(ch);
		init();
	}

	public MuxChannel(ProtocolStackConfigurator configurator) throws ChannelException {
		super(configurator);
		init();
	}

	public MuxChannel(String properties) throws ChannelException {
		super(properties);
		init();
	}

	public MuxChannel(URL properties) throws ChannelException {
		super(properties);
		init();
	}

	private void init() {
		super.setUpHandler(new MuxUpHandler());
	}

	@Override
	public void setUpHandler(UpHandler upHandler) {
		if(upHandler instanceof MuxUpHandler) {
			super.setUpHandler(upHandler);
		} else {
			super.setUpHandler(new MuxUpHandler(upHandler));
		}
	}
}
