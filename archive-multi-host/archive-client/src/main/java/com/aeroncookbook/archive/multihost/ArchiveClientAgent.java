package com.aeroncookbook.archive.multihost;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.TimeoutException;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class ArchiveClientAgent implements Agent
{
    public static final String AERON_UDP_ENDPOINT = "aeron:udp?endpoint=";
    private static final int RECORDED_STREAM_ID = 100;
    private static final int REPLAY_STREAM_ID = 200;
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveClientAgent.class);
    private final String archiveHost;
    private final MediaDriver mediaDriver;
    private final String thisHost;
    private final int archiveControlPort;
    private final int archiveEventPort;
    private final ArchiveClientFragmentHandler fragmentHandler;
    private final Aeron aeron;
    private final IdleStrategy idleStrategy;
    private AeronArchive archive;
    private AeronArchive.AsyncConnect asyncConnect;
    private State currentState;
    private Subscription replayDestinationSubs;

    public ArchiveClientAgent(final String archiveHost, final String thisHost, final int archiveControlPort,
        final int archiveEventPort, final ArchiveClientFragmentHandler fragmentHandler)
    {
        this.archiveHost = archiveHost;
        this.thisHost = localHost(thisHost);
        this.archiveControlPort = archiveControlPort;
        this.archiveEventPort = archiveEventPort;
        this.fragmentHandler = fragmentHandler;
        this.idleStrategy = new SleepingMillisIdleStrategy(250);

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

        this.currentState = State.AERON_READY;
    }

    @Override
    public int doWork()
    {
        switch (currentState)
        {
            case AERON_READY -> connectToArchive();
            case POLLING_SUBSCRIPTION -> replayDestinationSubs.poll(fragmentHandler, 100);
            default -> LOGGER.error("unknown state {}", currentState);
        }

        return 0;
    }

    private void connectToArchive()
    {
        //start an asyncConnect if one not in progress
        if (asyncConnect == null)
        {
            LOGGER.info("connecting aeron archive");
            asyncConnect = AeronArchive.asyncConnect(new AeronArchive.Context()
                .controlRequestChannel(AERON_UDP_ENDPOINT + archiveHost + ":" + archiveControlPort)
                .recordingEventsChannel(AERON_UDP_ENDPOINT + archiveHost + ":" + archiveEventPort)
                .controlResponseChannel(AERON_UDP_ENDPOINT + thisHost + ":0")
                .aeron(aeron));
        }
        else
        {
            //if the archive hasn't been set yet, poll it after idling 250ms
            if (null == archive)
            {
                LOGGER.info("awaiting aeron archive");
                idleStrategy.idle();
                try
                {
                    archive = asyncConnect.poll();
                }
                catch (final TimeoutException e)
                {
                    LOGGER.info("timeout");
                    asyncConnect = null;
                }
            }
            else
            {
                LOGGER.info("finding remote recording");
                //archive is connected. find the recording on the remote archive host
                final var recordingId = getRecordingId("aeron:ipc", RECORDED_STREAM_ID);
                if (recordingId != Long.MIN_VALUE)
                {
                    //ask aeron to assign an ephemeral port for this replay
                    final var localReplayChannelEphemeral = AERON_UDP_ENDPOINT + thisHost + ":0";
                    //construct a local subscription for the remote host to replay to
                    replayDestinationSubs = aeron.addSubscription(localReplayChannelEphemeral, REPLAY_STREAM_ID);
                    //resolve the actual port and use that for the replay
                    final var actualReplayChannel = replayDestinationSubs.tryResolveChannelEndpointPort();
                    LOGGER.info("actualReplayChannel={}", actualReplayChannel);
                    //replay from the archive recording the start
                    final long replaySession =
                        archive.startReplay(recordingId, 0L, Long.MAX_VALUE, actualReplayChannel, REPLAY_STREAM_ID);
                    LOGGER.info("ready to poll subscription, replaying to {}, image is {}", actualReplayChannel,
                        (int)replaySession);
                    currentState = State.POLLING_SUBSCRIPTION;
                }
                else
                {
                    //await the remote host being ready, idle 250ms
                    idleStrategy.idle();
                }
            }
        }
    }

    private long getRecordingId(final String remoteRecordedChannel, final int remoteRecordedStream)
    {
        final var lastRecordingId = new MutableLong();
        final RecordingDescriptorConsumer consumer = (controlSessionId, correlationId, recordingId,
            startTimestamp, stopTimestamp, startPosition,
            stopPosition, initialTermId, segmentFileLength,
            termBufferLength, mtuLength, sessionId,
            streamId, strippedChannel, originalChannel,
            sourceIdentity) -> lastRecordingId.set(recordingId);

        final var fromRecordingId = 0L;
        final var recordCount = 100;

        final int foundCount = archive.listRecordingsForUri(fromRecordingId, recordCount, remoteRecordedChannel,
            remoteRecordedStream, consumer);

        if (0 == foundCount)
        {
            return Long.MIN_VALUE;
        }

        return lastRecordingId.get();
    }

    @Override
    public String roleName()
    {
        return "archive-client";
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
        catch (final SocketException e)
        {
            LOGGER.info("Failed to get address");
        }
        return fallback;
    }

    @Override
    public void onClose()
    {
        Agent.super.onClose();
        LOGGER.info("shutting down");
        CloseHelper.quietClose(replayDestinationSubs);
        CloseHelper.quietClose(archive);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }

}
