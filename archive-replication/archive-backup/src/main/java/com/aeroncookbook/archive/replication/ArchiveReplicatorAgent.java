package com.aeroncookbook.archive.replication;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.client.RecordingEventsAdapter;
import io.aeron.archive.client.RecordingSignalAdapter;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.TimeoutException;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Objects;

import static io.aeron.Aeron.NULL_VALUE;

public class ArchiveReplicatorAgent implements Agent
{
    public static final String AERON_UDP_ENDPOINT = "aeron:udp?endpoint=";
    private static final EpochClock CLOCK = SystemEpochClock.INSTANCE;
    private static final int STREAM_ID = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveReplicatorAgent.class);
    private final ArchivingMediaDriver archivingMediaDriver;
    private final Aeron aeron;
    private final IdleStrategy idleStrategy;
    private final String host;
    private final String remoteArchiveHost;
    private final int controlChannelPort;
    private final int recordingEventsPort;
    private final int replayChannelPort;
    private RecordingSignalAdapter recordingSignalAdapter;
    private RecordingEventsAdapter recordingEventsAdapter;
    private AeronArchive localArchiveClient;
    private AeronArchive remoteArchiveClient;
    private AeronArchive.AsyncConnect remoteAsyncConnect;
    private State currentState;

    public ArchiveReplicatorAgent(final String host, final String remoteArchiveHost, final int controlChannelPort,
        final int recordingEventsPort, final int replayChannelPort)
    {
        this.host = localHost(host);
        this.remoteArchiveHost = remoteArchiveHost;
        this.controlChannelPort = controlChannelPort;
        this.recordingEventsPort = recordingEventsPort;
        this.replayChannelPort = replayChannelPort;
        this.idleStrategy = new SleepingMillisIdleStrategy(250);
        this.archivingMediaDriver = launchMediaDriver(host, controlChannelPort, recordingEventsPort, replayChannelPort);
        this.aeron = launchAeron(archivingMediaDriver);
        LOGGER.info("Media Driver directory is {}; Archive directory is {}",
            archivingMediaDriver.mediaDriver().aeronDirectoryName(),
            archivingMediaDriver.archive().context().archiveDirectoryName());
        currentState = State.AERON_READY;
    }

    private Aeron launchAeron(final ArchivingMediaDriver archivingMediaDriver)
    {
        LOGGER.info("launching aeron");
        return Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(archivingMediaDriver.mediaDriver().aeronDirectoryName())
            .errorHandler(this::errorHandler)
            .idleStrategy(new SleepingMillisIdleStrategy()));
    }

    private ArchivingMediaDriver launchMediaDriver(final String thisHost, final int controlChannelPort,
        final int recordingEventsPort, final int replayChannelPort)
    {
        LOGGER.info("launching ArchivingMediaDriver");
        final String controlChannel = AERON_UDP_ENDPOINT + thisHost + ":" + controlChannelPort;
        final String controlResponseChannel = AERON_UDP_ENDPOINT + thisHost + ":0";
        final String replicationChannel = AERON_UDP_ENDPOINT + thisHost + ":" + replayChannelPort;
        final String recordingEventsChannel =
            "aeron:udp?control-mode=dynamic|control=" + thisHost + ":" + recordingEventsPort;

        final var archiveContext = new Archive.Context()
            .deleteArchiveOnStart(true)
            .errorHandler(this::errorHandler)
            .controlChannel(controlChannel)
            .recordingEventsChannel(recordingEventsChannel)
            .idleStrategySupplier(SleepingMillisIdleStrategy::new)
            .replicationChannel(replicationChannel) //required to allow replication to this Archive
            .threadingMode(ArchiveThreadingMode.SHARED);

        //this is required for the AeronArchive client within the Aeron Archive host process for replication
        final var archiveClientContext = new AeronArchive.Context()
            .controlResponseChannel(controlResponseChannel);

        archiveContext.archiveClientContext(archiveClientContext);

        final var mediaDriverContext = new MediaDriver.Context()
            .spiesSimulateConnection(true)
            .errorHandler(this::errorHandler)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new SleepingMillisIdleStrategy())
            .dirDeleteOnStart(true);

        return ArchivingMediaDriver.launch(mediaDriverContext, archiveContext);
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
        switch (currentState)
        {
            case AERON_READY -> connectLocalArchive();
            case ARCHIVE_READY -> connectAndReplicateRemoteArchive();
            case REPLICATING -> idleStrategy.idle(); //aeron is doing all the replication work
            default -> LOGGER.info("unknown state {}", currentState);
        }

        if (recordingSignalAdapter != null)
        {
            recordingSignalAdapter.poll();
        }

        if (recordingEventsAdapter != null)
        {
            recordingEventsAdapter.poll();
        }

        return 0;
    }

    private void connectAndReplicateRemoteArchive()
    {
        if (remoteAsyncConnect == null)
        {
            LOGGER.info("connecting aeron archive");
            remoteAsyncConnect = AeronArchive.asyncConnect(new AeronArchive.Context()
                .controlRequestChannel(AERON_UDP_ENDPOINT + remoteArchiveHost + ":" + controlChannelPort)
                .recordingEventsChannel(AERON_UDP_ENDPOINT + remoteArchiveHost + ":" + recordingEventsPort)
                .controlResponseChannel(AERON_UDP_ENDPOINT + host + ":0")
                .aeron(aeron));
        }
        else
        {
            //if the archive hasn't been set yet, poll it after idling 250ms
            if (null == remoteArchiveClient)
            {
                LOGGER.info("awaiting aeron archive");
                idleStrategy.idle();
                try
                {
                    remoteArchiveClient = remoteAsyncConnect.poll();
                }
                catch (final TimeoutException e)
                {
                    LOGGER.info("timeout");
                    remoteAsyncConnect = null;
                }
            }
            else
            {
                LOGGER.info("finding remote recording");
                //archive is connected. find the recording on the remote archive host
                final var recordingId = getRemoteRecordingId("aeron:ipc", STREAM_ID);
                if (recordingId != Long.MIN_VALUE)
                {
                    LOGGER.info("remote recording id is {}", recordingId);
                    final long replicationId = localArchiveClient.replicate(recordingId, NULL_VALUE,
                        remoteArchiveClient.context().controlRequestStreamId(),
                        remoteArchiveClient.context().controlRequestChannel(), "");
                    LOGGER.info("replication id is {}", replicationId);
                    currentState = State.REPLICATING;
                }
                else
                {
                    //await the remote host being ready, idle 250ms
                    idleStrategy.idle();
                }
            }
        }
    }

    private void connectLocalArchive()
    {
        Objects.requireNonNull(archivingMediaDriver);
        Objects.requireNonNull(aeron);

        final var localControlRequestChannel = AERON_UDP_ENDPOINT + host + ":" + controlChannelPort;
        final var localControlResponseChannel = AERON_UDP_ENDPOINT + host + ":0";
        final var localRecordingEventsChannel = "aeron:udp?control-mode=dynamic|control=" + host + ":" +
            recordingEventsPort;


        LOGGER.info("connecting local archive");

        localArchiveClient = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .controlRequestChannel(localControlRequestChannel)
            .controlResponseChannel(localControlResponseChannel)
            .recordingEventsChannel(localRecordingEventsChannel)
            .idleStrategy(new SleepingMillisIdleStrategy()));

        final var activityListener = new ArchiveActivityListener();
        recordingSignalAdapter = new RecordingSignalAdapter(localArchiveClient.controlSessionId(), activityListener,
            activityListener, localArchiveClient.controlResponsePoller().subscription(), 10);

        final var recordingChannel = localArchiveClient.context().recordingEventsChannel();
        final var recordingStreamId = localArchiveClient.context().recordingEventsStreamId();
        final var recordingEvents = aeron.addSubscription(recordingChannel, recordingStreamId);
        final var progressListener = new ArchiveProgressListener();
        recordingEventsAdapter = new RecordingEventsAdapter(progressListener, recordingEvents, 10);

        this.currentState = State.ARCHIVE_READY;
    }

    private long getRemoteRecordingId(final String remoteRecordedChannel, final int remoteRecordedStream)
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

        final int foundCount = remoteArchiveClient.listRecordingsForUri(fromRecordingId, recordCount,
            remoteRecordedChannel, remoteRecordedStream, consumer);

        if (0 == foundCount)
        {
            return Long.MIN_VALUE;
        }

        return lastRecordingId.get();
    }

    @Override
    public void onClose()
    {
        Agent.super.onClose();
        this.currentState = State.SHUTTING_DOWN;
        LOGGER.info("Shutting down");
        CloseHelper.quietClose(remoteArchiveClient);
        CloseHelper.quietClose(localArchiveClient);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(archivingMediaDriver);
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
    public String roleName()
    {
        return "agent-replicator";
    }

}
