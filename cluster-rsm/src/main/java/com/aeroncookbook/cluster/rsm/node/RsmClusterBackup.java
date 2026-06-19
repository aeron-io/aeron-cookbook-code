package com.aeroncookbook.cluster.rsm.node;

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackupEventsListener;
import io.aeron.cluster.ClusterBackupMediaDriver;
import io.aeron.cluster.ClusterMember;
import io.aeron.cluster.RecordingLog;
import io.aeron.samples.cluster.ClusterConfig;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.util.Arrays;
import java.util.List;

import static io.aeron.cluster.ClusterBackup.Configuration.ReplayStart.BEGINNING;
import static io.aeron.cluster.ClusterBackup.SourceType.ANY;

public class RsmClusterBackup
{
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        final int clusterMemberNodeId = 0;
        final int backupNodeId = 1;

        final ClusterConfig clusterConfig = ClusterConfig.create(
            backupNodeId, List.of("localhost", "localhost"), List.of("localhost", "localhost"), 9000,
            new RsmClusteredService());

        clusterConfig.mediaDriverContext().errorHandler(errorHandler("Media Driver"));
        clusterConfig.archiveContext().errorHandler(errorHandler("Archive"));
        clusterConfig.aeronArchiveContext().errorHandler(errorHandler("Aeron Archive"));
        clusterConfig.consensusModuleContext().errorHandler(errorHandler("Consensus Module"));
        clusterConfig.clusteredServiceContext().errorHandler(errorHandler("Clustered Service"));
        clusterConfig.consensusModuleContext().ingressChannel("aeron:udp?endpoint=localhost:9010|term-length=64k");
        clusterConfig.consensusModuleContext().deleteDirOnStart(false); //true to always start fresh

        final String consensusEndpoints = "localhost:" + ClusterConfig.calculatePort(
            clusterMemberNodeId, 9000, ClusterConfig.MEMBER_FACING_PORT_OFFSET);
        final String clusterArchiveEndpoint = "localhost:" + ClusterConfig.calculatePort(
            clusterMemberNodeId, 9000, ClusterConfig.ARCHIVE_CONTROL_PORT_OFFSET);

        final String catchupEndpoint = "localhost:" + ClusterConfig.calculatePort(
            backupNodeId, 9000, ClusterConfig.TRANSFER_PORT_OFFSET);
        final String consensusChannel = "aeron:udp?endpoint=localhost:" + ClusterConfig.calculatePort(
            backupNodeId, 9000, ClusterConfig.MEMBER_FACING_PORT_OFFSET);

        final AeronArchive.Context clusterArchiveContext = clusterConfig.aeronArchiveContext().clone()
            .controlRequestChannel("aeron:udp?endpoint=" + clusterArchiveEndpoint)
            .controlResponseChannel("aeron:udp?endpoint=localhost:0");

        final ClusterBackup.Context backupContext = new ClusterBackup.Context()
            .aeronDirectoryName(clusterConfig.mediaDriverContext().aeronDirectoryName())
            .clusterDir(clusterConfig.consensusModuleContext().clusterDir())
            .archiveContext(clusterConfig.aeronArchiveContext())
            .clusterArchiveContext(clusterArchiveContext)
            .clusterConsensusEndpoints(consensusEndpoints)
            .consensusChannel(consensusChannel)
            .catchupEndpoint(catchupEndpoint)
            .sourceType(ANY)
            .initialReplayStart(BEGINNING)
            .credentialsSupplier(clusterConfig.aeronArchiveContext().credentialsSupplier())
            .eventsListener(new PrintingClusterBackupEventsListener());

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
            ClusterBackupMediaDriver clusteredMediaDriver = ClusterBackupMediaDriver.launch(
                clusterConfig.mediaDriverContext().terminationHook(barrier::signalAll),
                clusterConfig.archiveContext(),
                backupContext))
        {
            System.out.println("Started Cluster Backup Node...");
            System.out.println("Cluster directory is " + backupContext.clusterDir());
            barrier.await();
            System.out.println("Exiting");
        }
    }

    private static ErrorHandler errorHandler(final String context)
    {
        return (Throwable throwable) ->
        {
            System.err.println(context);
            throwable.printStackTrace(System.err);
        };
    }

    private static final class PrintingClusterBackupEventsListener implements ClusterBackupEventsListener
    {
        public void onBackupQuery()
        {
            System.out.println("onBackupQuery");
        }

        public void onPossibleFailure(final Exception ex)
        {
            System.out.println("onPossibleFailure(" + ex.getMessage() + ")");
        }

        public void onBackupResponse(
            final ClusterMember[] clusterMembers,
            final ClusterMember logSourceMember,
            final List<RecordingLog.Snapshot> snapshotsToRetrieve)
        {
            System.out.println(
                "onBackupResponse(" +
                "\n\t" + Arrays.toString(clusterMembers) + ", " +
                "\n\t" + logSourceMember + ", " +
                "\n\t" + snapshotsToRetrieve + ")");
        }

        public void onUpdatedRecordingLog(
            final RecordingLog recordingLog,
            final List<RecordingLog.Snapshot> snapshotsRetrieved)
        {
            System.out.println(
                "onUpdatedRecordingLog(" +
                "\n\t" + recordingLog + ", " +
                "\n\t" + snapshotsRetrieved + ")");
        }

        public void onLiveLogProgress(
            final long recordingId,
            final long recordingPosCounterId,
            final long logPosition)
        {
            System.out.println(
                "onLiveLogProgress(" + recordingId + ", " + recordingPosCounterId + ", " + logPosition + ")");
        }
    }
}
