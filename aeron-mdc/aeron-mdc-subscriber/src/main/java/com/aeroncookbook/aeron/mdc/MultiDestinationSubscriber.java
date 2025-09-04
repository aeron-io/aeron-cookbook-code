package com.aeroncookbook.aeron.mdc;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiDestinationSubscriber
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiDestinationSubscriber.class);

    public static void main(final String[] args)
    {
        final var thisHost = System.getenv().get("THISHOST");
        final var mdcHost = System.getenv().get("MDCHOST");
        final var controlPort = System.getenv().get("CONTROLPORT");

        if (mdcHost == null || controlPort == null || thisHost == null)
        {
            LOGGER.error("env vars required: THISHOST, MDCHOST, CONTROLPORT");
        }
        else
        {
            final var controlChannelPort = Integer.parseInt(controlPort);
            final MultiDestinationSubscriberAgent hostAgent =
                new MultiDestinationSubscriberAgent(mdcHost, thisHost, controlChannelPort);
            final SleepingMillisIdleStrategy idleStrategy = new SleepingMillisIdleStrategy();
            try (var barrier = new ShutdownSignalBarrier();
                var runner = new AgentRunner(idleStrategy, MultiDestinationSubscriber::errorHandler, null, hostAgent))
            {
                AgentRunner.startOnThread(runner);

                barrier.await();
            }
        }
    }

    private static void errorHandler(final Throwable throwable)
    {
        LOGGER.error("agent error {}", throwable.getMessage(), throwable);
    }
}
