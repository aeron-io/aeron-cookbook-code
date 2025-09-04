package com.aeroncookbook.archive.replication;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveClient
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveClient.class);

    public static void main(final String[] args)
    {
        final var thisHost = System.getenv().get("THISHOST");
        final var archiveHost = System.getenv().get("ARCHIVEHOST");
        final var backupHost = System.getenv().get("BACKUPHOST");
        final var controlPort = System.getenv().get("CONTROLPORT");
        final var eventPort = System.getenv().get("EVENTSPORT");

        if (archiveHost == null || controlPort == null || thisHost == null || eventPort == null)
        {
            LOGGER.error("env vars required: THISHOST, ARCHIVEHOST, BACKUPHOST, CONTROLPORT, EVENTSPORT");
        }
        else
        {
            final var controlChannelPort = Integer.parseInt(controlPort);
            final var eventChannelPort = Integer.parseInt(eventPort);
            final var fragmentHandler = new ArchiveClientFragmentHandler();
            final ArchiveClientAgent hostAgent =
                new ArchiveClientAgent(archiveHost, thisHost, backupHost, controlChannelPort, eventChannelPort,
                fragmentHandler);
            final SleepingMillisIdleStrategy idleStrategy = new SleepingMillisIdleStrategy();
            try (var barrier = new ShutdownSignalBarrier();
                var runner = new AgentRunner(idleStrategy, ArchiveClient::errorHandler, null, hostAgent))
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
