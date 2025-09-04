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

package com.aeroncookbook.ipc;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class SimplestCase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SimplestCase.class);

    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        final String channel = "aeron:ipc";
        final String message = "my message";

        final IdleStrategy idle = new SleepingIdleStrategy();
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));

        try (MediaDriver driver = MediaDriver.launch();
            Aeron aeron = Aeron.connect();
            Subscription sub = aeron.addSubscription(channel, 10);
            Publication pub = aeron.addPublication(channel, 10))
        {
            while (!pub.isConnected())
            {
                idle.idle();
            }

            unsafeBuffer.putStringAscii(0, message);
            LOGGER.info("sending:{}", message);
            while (pub.offer(unsafeBuffer) < 0)
            {
                idle.idle();
            }

            final FragmentHandler handler = (buffer, offset, length, header) ->
                LOGGER.info("received:{}", buffer.getStringAscii(offset));

            while (sub.poll(handler, 1) <= 0)
            {
                idle.idle();
            }
        }
    }
}
