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

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

public class SendAgent2 implements Agent
{
    private final int sendCount;
    private final ManyToOneRingBuffer ringBuffer;
    private int currentCountItem;

    public SendAgent2(final ManyToOneRingBuffer ringBuffer, final int sendCount)
    {
        this.currentCountItem = sendCount * 2;
        this.ringBuffer = ringBuffer;
        this.sendCount = sendCount;
    }

    @Override
    public int doWork()
    {
        if (currentCountItem < sendCount)
        {
            return 0;
        }

        final int claimIndex = ringBuffer.tryClaim(1, Integer.BYTES);
        if (claimIndex > 0)
        {
            currentCountItem -= 1;
            final AtomicBuffer buffer = ringBuffer.buffer();
            buffer.putInt(claimIndex, currentCountItem);
            ringBuffer.commit(claimIndex);
        }
        return 0;
    }

    @Override
    public String roleName()
    {
        return "sender2";
    }
}
