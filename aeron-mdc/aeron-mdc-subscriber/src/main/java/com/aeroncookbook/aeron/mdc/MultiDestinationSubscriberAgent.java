package com.aeroncookbook.aeron.mdc;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

public class MultiDestinationSubscriberAgent implements Agent
{
    private static final int STREAM_ID = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiDestinationSubscriberAgent.class);
    private final MediaDriver mediaDriver;
    private final MultiDestinationSubscriberFragmentHandler fragmentHandler;
    private final Aeron aeron;
    private Subscription mdcSubscription;

    public MultiDestinationSubscriberAgent(final String mdcHost, final String thisHost, final int controlPort)
    {
        this.fragmentHandler = new MultiDestinationSubscriberFragmentHandler();

        LOGGER.info("launching media driver");
        //launch a media driver
        this.mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new SleepingMillisIdleStrategy()));

        LOGGER.info("connecting aeron; media driver directory {}", mediaDriver.aeronDirectoryName());
        //connect an aeron client
        this.aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .idleStrategy(new SleepingMillisIdleStrategy()));

        //add the MDC subscription
        final var channel = "aeron:udp?endpoint=" + localHost(thisHost) +
            ":12001|control=" + mdcHost + ":" + controlPort + "|control-mode=dynamic";
        LOGGER.info("adding the subscription to channel: {}", channel);
        mdcSubscription = aeron.addSubscription(channel, STREAM_ID);
    }

    @Override
    public int doWork()
    {
        mdcSubscription.poll(fragmentHandler, 100);
        return 0;
    }

    @Override
    public String roleName()
    {
        return "mdc-subscriber";
    }

    @Override
    public void onStart()
    {
        Agent.super.onStart();
        LOGGER.info("starting");
    }

    public static String localHost(final String fallback)
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
    public void onClose()
    {
        Agent.super.onClose();
        LOGGER.info("shutting down");
        CloseHelper.quietClose(mdcSubscription);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }

}
