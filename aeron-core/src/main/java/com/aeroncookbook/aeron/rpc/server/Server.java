/*
 * Copyright 2019-2023 Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aeroncookbook.aeron.rpc.server;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    public static void main(final String[] args)
    {
        final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        //construct Media Driver, cleaning up media driver folder on start/stop
        final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.getAeronDirectoryName() + "-server")
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED);
        final MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaDriverCtx);

        //construct Aeron, pointing at the media driver's folder
        final Aeron.Context aeronCtx = new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName());
        final Aeron aeron = Aeron.connect(aeronCtx);


        LOGGER.info("Dir {}", mediaDriver.aeronDirectoryName());
        //Construct the server agent
        final ServerAgent serverAgent = new ServerAgent(aeron, barrier);
        final AgentRunner serverAgentRunner = new AgentRunner(idleStrategy, Throwable::printStackTrace,
            null, serverAgent);
        AgentRunner.startOnThread(serverAgentRunner);

        //Await shutdown signal
        barrier.await();

        CloseHelper.quietClose(serverAgentRunner);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
        CloseHelper.quietClose(barrier);
    }
}
