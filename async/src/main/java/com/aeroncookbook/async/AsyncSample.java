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

package com.aeroncookbook.async;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

public class AsyncSample
{
    public static void main(final String[] args)
    {
        final String channel = "aeron:ipc";
        final int streamToTimer = 10;
        final int streamToTimerClient = 11;
        final IdleStrategy idleStrategySend = new SleepingMillisIdleStrategy(7);
        final IdleStrategy idleStrategyReceive = new SleepingMillisIdleStrategy(5);
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        //construct Media Driver, cleaning up media driver folder on start/stop
        final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);
        final MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaDriverCtx);

        //construct Aeron, pointing at the media driver's folder
        final Aeron.Context aeronCtx = new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName());
        final Aeron aeron = Aeron.connect(aeronCtx);

        //construct the subs and pubs
        final Subscription timerSubscription = aeron.addSubscription(channel, streamToTimer);
        final Publication timerPublication = aeron.addPublication(channel, streamToTimer);
        final Subscription timerClientSubscription = aeron.addSubscription(channel, streamToTimerClient);
        final Publication timerClientPublication = aeron.addPublication(channel, streamToTimerClient);

        //construct the agents
        final TimerClientAgent timerClientAgent =
            new TimerClientAgent(timerPublication, timerClientSubscription);
        final TimerAgent timerAgent = new TimerAgent(timerSubscription, timerClientPublication, barrier);

        //construct agent runners
        final AgentRunner timerClientAgentRunner = new AgentRunner(idleStrategySend,
            Throwable::printStackTrace, null, timerClientAgent);
        final AgentRunner timerAgentRunner = new AgentRunner(idleStrategyReceive,
            Throwable::printStackTrace, null, timerAgent);

        //start the runners
        AgentRunner.startOnThread(timerClientAgentRunner);
        AgentRunner.startOnThread(timerAgentRunner);

        //wait for the final item to be received before closing
        barrier.await();

        //close the resources
        CloseHelper.closeAll(timerAgentRunner, timerClientAgentRunner, aeron, mediaDriver, barrier);
    }
}
