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

package com.aeroncookbook.agrona.ringbuffer;

import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class StartHere
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StartHere.class);

    public static void main(final String[] args)
    {
        final int sendCount = 10_000_000;
        final int bufferLength = 16384 + RingBufferDescriptor.TRAILER_LENGTH;
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(bufferLength));
        final IdleStrategy idleStrategySend1 = new BusySpinIdleStrategy();
        final IdleStrategy idleStrategySend2 = new BusySpinIdleStrategy();
        final IdleStrategy idleStrategyReceive = new BusySpinIdleStrategy();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(unsafeBuffer);

        //construct the agents
        final SendAgent1 sendAgent1 = new SendAgent1(ringBuffer, sendCount);
        final SendAgent2 sendAgent2 = new SendAgent2(ringBuffer, sendCount);
        final ReceiveAgent receiveAgent = new ReceiveAgent(ringBuffer, barrier, sendCount);

        //construct agent runners
        final AgentRunner sendAgentRunner1 = new AgentRunner(idleStrategySend1,
            Throwable::printStackTrace, null, sendAgent1);
        final AgentRunner sendAgentRunner2 = new AgentRunner(idleStrategySend2,
            Throwable::printStackTrace, null, sendAgent2);
        final AgentRunner receiveAgentRunner = new AgentRunner(idleStrategyReceive,
            Throwable::printStackTrace, null, receiveAgent);
        LOGGER.info("starting");
        //start the runners
        AgentRunner.startOnThread(sendAgentRunner1);
        AgentRunner.startOnThread(sendAgentRunner2);
        AgentRunner.startOnThread(receiveAgentRunner);

        //wait for the final item to be received before closing
        barrier.await();

        //close the resources
        CloseHelper.closeAll(receiveAgentRunner, sendAgentRunner1, sendAgentRunner2, barrier);
    }
}
