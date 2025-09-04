package com.aeroncookbook.archive.multihost;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingEventsAdapter;
import io.aeron.archive.client.RecordingSignalAdapter;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Objects;

public class ArchiveHostAgent implements Agent
{
    public static final String AERON_UDP_ENDPOINT = "aeron:udp?endpoint=";
    private static final EpochClock CLOCK = SystemEpochClock.INSTANCE;
    private static final int STREAM_ID = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveHostAgent.class);
    private final ArchivingMediaDriver archivingMediaDriver;
    private final Aeron aeron;
    private final IdleStrategy idleStrategy;
    private final String host;
    private final int controlChannelPort;
    private final int recordingEventsPort;
    private final MutableDirectBuffer mutableDirectBuffer;
    private RecordingSignalAdapter recordingSignalAdapter;
    private RecordingEventsAdapter recordingEventsAdapter;
    private AeronArchive archive;
    private Publication publication;
    private long nextAppend = Long.MIN_VALUE;
    private long lastSeq = 0;
    private State currentState;

    public ArchiveHostAgent(final String host, final int controlChannelPort, final int recordingEventsPort)
    {
        this.host = localHost(host);
        this.controlChannelPort = controlChannelPort;
        this.recordingEventsPort = recordingEventsPort;
        this.idleStrategy = new SleepingMillisIdleStrategy();
        this.archivingMediaDriver = launchMediaDriver(host, controlChannelPort, recordingEventsPort);
        this.mutableDirectBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Long.BYTES));
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

    private ArchivingMediaDriver launchMediaDriver(final String host, final int controlChannelPort,
        final int recordingEventsPort)
    {
        LOGGER.info("launching ArchivingMediaDriver");
        final String controlChannel = AERON_UDP_ENDPOINT + host + ":" + controlChannelPort;
        final String replicationChannel = AERON_UDP_ENDPOINT + host + ":" + controlChannelPort + 1;
        final String recordingEventsChannel =
            "aeron:udp?control-mode=dynamic|control=" + host + ":" + recordingEventsPort;

        final var archiveContext = new Archive.Context()
            .deleteArchiveOnStart(true)
            .errorHandler(this::errorHandler)
            .controlChannel(controlChannel)
            .replicationChannel(replicationChannel)
            .recordingEventsChannel(recordingEventsChannel)
            .idleStrategySupplier(SleepingMillisIdleStrategy::new)
            .threadingMode(ArchiveThreadingMode.SHARED);

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
            case AERON_READY -> createArchiveAndRecord();
            case ARCHIVE_READY -> appendData();
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

    private void appendData()
    {
        if (CLOCK.time() >= nextAppend)
        {
            lastSeq += 1;
            mutableDirectBuffer.putLong(0, lastSeq);
            publication.offer(mutableDirectBuffer, 0, Long.BYTES);
            nextAppend = CLOCK.time() + 2000;
            LOGGER.info("appended {}", lastSeq);
        }
    }

    private void createArchiveAndRecord()
    {
        Objects.requireNonNull(archivingMediaDriver);
        Objects.requireNonNull(aeron);

        final var controlRequestChannel = AERON_UDP_ENDPOINT + host + ":" + controlChannelPort;
        final var controlResponseChannel = AERON_UDP_ENDPOINT + host + ":0";
        final var recordingEventsChannel = "aeron:udp?control-mode=dynamic|control=" + host + ":" + recordingEventsPort;

        LOGGER.info("creating archive");

        archive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .controlRequestChannel(controlRequestChannel)
            .controlResponseChannel(controlResponseChannel)
            .recordingEventsChannel(recordingEventsChannel)
            .idleStrategy(new SleepingMillisIdleStrategy()));

        LOGGER.info("creating publication");

        publication = aeron.addExclusivePublication("aeron:ipc", STREAM_ID);

        LOGGER.info("starting recording");

        archive.startRecording("aeron:ipc", STREAM_ID, SourceLocation.LOCAL);

        LOGGER.info("waiting for recording to start for session {}", publication.sessionId());
        final var counters = aeron.countersReader();
        int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archive.archiveId());
        while (CountersReader.NULL_COUNTER_ID == counterId)
        {
            idleStrategy.idle();
            counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archive.archiveId());
        }
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);

        final var activityListener = new ArchiveActivityListener();
        recordingSignalAdapter = new RecordingSignalAdapter(archive.controlSessionId(), activityListener,
            activityListener, archive.controlResponsePoller().subscription(), 10);

        final var recordingChannel = archive.context().recordingEventsChannel();
        final var recordingStreamId = archive.context().recordingEventsStreamId();
        final var recordingEvents = aeron.addSubscription(recordingChannel, recordingStreamId);
        final var progressListener = new ArchiveProgressListener();
        recordingEventsAdapter = new RecordingEventsAdapter(progressListener, recordingEvents, 10);

        LOGGER.info("archive recording started; recording id is {}", recordingId);

        this.currentState = State.ARCHIVE_READY;
    }

    @Override
    public void onClose()
    {
        Agent.super.onClose();
        this.currentState = State.SHUTTING_DOWN;
        LOGGER.info("Shutting down");
        CloseHelper.quietClose(publication);
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
        return "agent-host";
    }

}
