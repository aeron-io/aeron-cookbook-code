package com.aeroncookbook.aeron.mdc;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.util.Enumeration;

public class MultiDestinationPublisherAgent implements Agent
{
    private static final EpochClock CLOCK = SystemEpochClock.INSTANCE;
    private static final int STREAM_ID = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiDestinationPublisherAgent.class);
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final MutableDirectBuffer mutableDirectBuffer;
    private final Publication publication;
    private long nextAppend = Long.MIN_VALUE;
    private long lastSeq = 0;

    @SuppressWarnings("this-escape")
    public MultiDestinationPublisherAgent(final String host, final int controlChannelPort)
    {
        this.mediaDriver = launchMediaDriver();
        this.mutableDirectBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Long.BYTES));

        this.aeron = launchAeron(mediaDriver);
        LOGGER.info("Media Driver directory is {}", mediaDriver.aeronDirectoryName());
        final var publicationChannel = "aeron:udp?control-mode=dynamic|control=" + localHost(host) +
            ":" + controlChannelPort;
        LOGGER.info("creating publication");
        publication = aeron.addExclusivePublication(publicationChannel, STREAM_ID);
    }

    private Aeron launchAeron(final MediaDriver mediaDriver)
    {
        LOGGER.info("launching aeron");
        return Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .errorHandler(this::errorHandler)
            .idleStrategy(new SleepingMillisIdleStrategy()));
    }

    private MediaDriver launchMediaDriver()
    {
        LOGGER.info("launching media driver");
        final var mediaDriverContext = new MediaDriver.Context()
            .spiesSimulateConnection(true)
            .errorHandler(this::errorHandler)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new SleepingMillisIdleStrategy())
            .dirDeleteOnStart(true);

        return MediaDriver.launch(mediaDriverContext);
    }

    private void errorHandler(final Throwable throwable)
    {
        LOGGER.error("unexpected failure {}", throwable.getMessage(), throwable);
    }

    @Override
    public void onStart()
    {
        LOGGER.info("Starting up");
        Agent.super.onStart();
    }

    @Override
    public int doWork()
    {
        if (CLOCK.time() >= nextAppend)
        {
            lastSeq += 1;
            mutableDirectBuffer.putLong(0, lastSeq);
            publication.offer(mutableDirectBuffer, 0, Long.BYTES);
            nextAppend = CLOCK.time() + 2000;
            LOGGER.info("appended {}", lastSeq);
        }

        return 0;
    }

    @Override
    public void onClose()
    {
        Agent.super.onClose();
        LOGGER.info("Shutting down");
        CloseHelper.quietClose(publication);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }

    public String localHost(final String fallback)
    {
        try
        {
            final Enumeration<NetworkInterface> interfaceEnumeration = NetworkInterface.getNetworkInterfaces();
            while (interfaceEnumeration.hasMoreElements())
            {
                final var networkInterface = interfaceEnumeration.nextElement();

                if (networkInterface.getName().startsWith("eth0"))
                {
                    final Enumeration<InetAddress> interfaceAddresses = networkInterface.getInetAddresses();
                    while (interfaceAddresses.hasMoreElements())
                    {
                        if (interfaceAddresses.nextElement() instanceof Inet4Address inet4Address)
                        {
                            LOGGER.info("detected ip4 address as {}", inet4Address.getHostAddress());
                            return inet4Address.getHostAddress();
                        }
                    }
                }
            }
        }
        catch (final Exception e)
        {
            LOGGER.info("Failed to get address, using {}", fallback);
        }
        return fallback;
    }

    @Override
    public String roleName()
    {
        return "mdc-publisher";
    }

}
