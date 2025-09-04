package com.aeroncookbook.archive.replication;

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

import static com.aeroncookbook.archive.replication.State.SWITCH_TO_BACKUP;

public class ArchiveClientAgent implements Agent
{
    public static final String AERON_UDP_ENDPOINT = "aeron:udp?endpoint=";
    private static final int RECORDED_STREAM_ID = 100;
    private static final int REPLAY_STREAM_ID = 200;
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveClientAgent.class);
    private final String archiveHost;
    private final MediaDriver mediaDriver;
    private final String thisHost;
    private final String backupHost;
    private final int archiveControlPort;
    private final int archiveEventPort;
    private final ArchiveClientFragmentHandler fragmentHandler;
    private final Aeron aeron;
    private final IdleStrategy idleStrategy;
    private long replaySession;
    private long backupStartPosition;
    private AeronArchive hostArchive;
    private AeronArchive backupArchive;
    private AeronArchive.AsyncConnect asyncHostConnect;
    private AeronArchive.AsyncConnect asyncBackupConnect;
    private State currentState;
    private Subscription replayDestinationSubs;
    private Subscription replayDestinationBackupSubs;

    public ArchiveClientAgent(
        final String archiveHost,
        final String thisHost,
        final String backupHost,
        final int archiveControlPort,
        final int archiveEventPort,
        final ArchiveClientFragmentHandler fragmentHandler)
    {
        this.archiveHost = archiveHost;
        this.thisHost = localHost(thisHost);
        this.backupHost = backupHost;
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

        this.currentState = State.AERON_CREATED;
    }

    @Override
    public int doWork()
    {
        switch (currentState)
        {
            case AERON_CREATED -> connectToHostArchive();
            case POLLING_SUBSCRIPTION -> initialReplayFromHost();
            case SWITCH_TO_BACKUP -> connectToBackupArchive();
            case POLLING_BACKUP_SUBSCRIPTION -> replayDestinationBackupSubs.poll(fragmentHandler, 100);
            default -> LOGGER.error("unknown state {}", currentState);
        }

        return 0;
    }

    private void connectToBackupArchive()
    {
        //start an asyncConnect if one not in progress
        if (asyncBackupConnect == null)
        {
            LOGGER.info("connecting to backup aeron archive");
            asyncBackupConnect = AeronArchive.asyncConnect(new AeronArchive.Context()
                .controlRequestChannel(AERON_UDP_ENDPOINT + backupHost + ":" + archiveControlPort)
                .recordingEventsChannel(AERON_UDP_ENDPOINT + backupHost + ":" + archiveEventPort)
                .controlResponseChannel(AERON_UDP_ENDPOINT + thisHost + ":0")
                .aeron(aeron));
        }
        else
        {
            //if the archive hasn't been set yet, poll it after idling 250ms
            if (null == backupArchive)
            {
                LOGGER.info("awaiting backup aeron archive");
                idleStrategy.idle();
                try
                {
                    backupArchive = asyncBackupConnect.poll();
                }
                catch (final TimeoutException e)
                {
                    LOGGER.info("timeout");
                    asyncBackupConnect = null;
                }
            }
            else
            {
                LOGGER.info("finding backup remote recording");
                //archive is connected. find the recording on the remote archive host
                final var recordingId = getRecordingId(backupArchive, "aeron:ipc", RECORDED_STREAM_ID);
                if (recordingId != Long.MIN_VALUE)
                {
                    //ask aeron to assign an ephemeral port for this replay
                    final var localReplayChannelEphemeral = AERON_UDP_ENDPOINT + thisHost + ":0";
                    //construct a local subscription for the remote host to replay to
                    replayDestinationBackupSubs = aeron.addSubscription(localReplayChannelEphemeral, REPLAY_STREAM_ID);
                    //resolve the actual port and use that for the replay
                    final var actualReplayChannel = replayDestinationBackupSubs.tryResolveChannelEndpointPort();
                    LOGGER.info("actualReplayChannel={}", actualReplayChannel);
                    //replay from the archive recording the backupStartPosition
                    replaySession =
                        backupArchive.startReplay(recordingId, backupStartPosition, Long.MAX_VALUE, actualReplayChannel,
                            REPLAY_STREAM_ID);
                    LOGGER.info("ready to poll subscription, replaying to {} from position {}, image is {}",
                        actualReplayChannel, backupStartPosition, (int)replaySession);
                    currentState = State.POLLING_BACKUP_SUBSCRIPTION;
                }
                else
                {
                    //await the remote host being ready, idle 250ms
                    idleStrategy.idle();
                }
            }
        }
    }

    private void initialReplayFromHost()
    {
        //fragment limit is set low to allow us to consume them one by one
        replayDestinationSubs.poll(fragmentHandler, 1);
        if (fragmentHandler.getLastValue() == 20)
        {
            LOGGER.info("replay has reached item 20 at position {}", fragmentHandler.getLastPosition());

            //stop the replay
            hostArchive.stopReplay(replaySession);

            //kill the replay subscription
            CloseHelper.quietClose(replayDestinationSubs);

            //kill the archive.
            CloseHelper.quietClose(asyncHostConnect);
            CloseHelper.quietClose(hostArchive);

            backupStartPosition = fragmentHandler.getLastPosition();

            this.currentState = SWITCH_TO_BACKUP;
        }
    }

    private void connectToHostArchive()
    {
        //start an asyncConnect if one not in progress
        if (asyncHostConnect == null)
        {
            LOGGER.info("connecting aeron archive");
            asyncHostConnect = AeronArchive.asyncConnect(new AeronArchive.Context()
                .controlRequestChannel(AERON_UDP_ENDPOINT + archiveHost + ":" + archiveControlPort)
                .recordingEventsChannel(AERON_UDP_ENDPOINT + archiveHost + ":" + archiveEventPort)
                .controlResponseChannel(AERON_UDP_ENDPOINT + thisHost + ":0")
                .aeron(aeron));
        }
        else
        {
            //if the archive hasn't been set yet, poll it after idling 250ms
            if (null == hostArchive)
            {
                LOGGER.info("awaiting aeron archive");
                idleStrategy.idle();
                try
                {
                    hostArchive = asyncHostConnect.poll();
                }
                catch (final TimeoutException e)
                {
                    LOGGER.info("timeout");
                    asyncHostConnect = null;
                }
            }
            else
            {
                LOGGER.info("finding remote recording");
                //archive is connected. find the recording on the remote archive host
                final var recordingId = getRecordingId(hostArchive, "aeron:ipc", RECORDED_STREAM_ID);
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
                    replaySession =
                        hostArchive.startReplay(recordingId, 0L, Long.MAX_VALUE, actualReplayChannel, REPLAY_STREAM_ID);
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

    private long getRecordingId(final AeronArchive srcArchive, final String remoteRecordedChannel,
        final int remoteRecordedStream)
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

        final int foundCount = srcArchive.listRecordingsForUri(fromRecordingId, recordCount, remoteRecordedChannel,
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
        CloseHelper.quietClose(replayDestinationBackupSubs);
        CloseHelper.quietClose(replayDestinationSubs);
        CloseHelper.quietClose(backupArchive);
        CloseHelper.quietClose(hostArchive);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }

}
