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

package com.aeroncookbook.agrona.agents;

import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class StartHere
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StartHere.class);

    public static void main(final String[] args)
    {
        final int sendCount = 18_000_000;
        final int bufferLength = 16384 + RingBufferDescriptor.TRAILER_LENGTH;
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(bufferLength));
        final IdleStrategy idleStrategySend = new BusySpinIdleStrategy();
        final IdleStrategy idleStrategyReceive = new BusySpinIdleStrategy();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final OneToOneRingBuffer ringBuffer = new OneToOneRingBuffer(unsafeBuffer);

        //construct the agents
        final SendAgent sendAgent = new SendAgent(ringBuffer, sendCount);
        final ReceiveAgent receiveAgent = new ReceiveAgent(ringBuffer, barrier, sendCount);

        //construct agent runners
        final AgentRunner sendAgentRunner = new AgentRunner(idleStrategySend,
            Throwable::printStackTrace, null, sendAgent);
        final AgentRunner receiveAgentRunner = new AgentRunner(idleStrategyReceive,
            Throwable::printStackTrace, null, receiveAgent);
        LOGGER.info("starting");
        //start the runners
        AgentRunner.startOnThread(sendAgentRunner);
        AgentRunner.startOnThread(receiveAgentRunner);

        //wait for the final item to be received before closing
        barrier.await();

        //close the resources
        CloseHelper.closeAll(receiveAgentRunner, sendAgentRunner, barrier);
    }
}
