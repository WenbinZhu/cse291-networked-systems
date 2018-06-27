package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.LogEntries;
import surfstore.SurfStoreBasic.LogEntry;
import surfstore.SurfStoreBasic.LogIndex;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.WriteResult;


public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;

    public MetadataStore(ConfigReader config) {
    	this.config = config;
	}

	private void start(int port, int servNum, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(config, servNum))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
        }

        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null) {
            throw new RuntimeException("Argument parsing failed");
        }
        
        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);
        int servNum = c_args.getInt("number");

        if (servNum > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(servNum), servNum, c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
        private int numMetaServer;
        private boolean isLeader;
        private volatile boolean crashed;
        private Map<String, Integer> versionMap;
        private Map<String, List<String>> blockListMap;

        private int lastApplied;
        private int[] nextIndex;
        private List<LogEntry> logs;

        private BlockStoreGrpc.BlockStoreBlockingStub blockStub;
        private MetadataStoreGrpc.MetadataStoreBlockingStub[] metaStubs;

        ScheduledExecutorService daemonExecutor;

        MetadataStoreImpl(ConfigReader config, int servNum) {
            this.numMetaServer = config.getNumMetadataServers();
            this.isLeader = servNum == config.getLeaderNum();
            this.versionMap = new HashMap<>();
            this.blockListMap = new HashMap<>();
            this.lastApplied = -1;
            this.nextIndex = new int[numMetaServer - 1];
            this.logs = new ArrayList<>();
            this.daemonExecutor = Executors.newSingleThreadScheduledExecutor();

            ManagedChannel bChannel = ManagedChannelBuilder.forAddress("127.0.0.1",
                    config.getBlockPort()).usePlaintext(true).build();
            this.blockStub = BlockStoreGrpc.newBlockingStub(bChannel);

            if (isLeader) {
                this.metaStubs = new MetadataStoreGrpc.MetadataStoreBlockingStub[numMetaServer - 1];
                for (int i = 1, j = 0; i <= numMetaServer; i++) {
                    if (i != servNum) {
                        ManagedChannel mChannel = ManagedChannelBuilder.forAddress("127.0.0.1",
                                config.getMetadataPort(i)).usePlaintext(true).build();
                        this.metaStubs[j++] = MetadataStoreGrpc.newBlockingStub(mChannel);
                    }
                }

                if (numMetaServer > 1) {
                    startAppendEntries();
                }
            }
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void readFile(FileInfo request, StreamObserver<FileInfo> responseObserver) {
            if (crashed) {
                responseObserver.onError(new RuntimeException("Server crashed!"));
                return;
            }

            FileInfo.Builder builder = FileInfo.newBuilder();
            builder.setFilename(request.getFilename());

            synchronized (this) {
                Integer version = versionMap.get(request.getFilename());
                List<String> blockList = blockListMap.get(request.getFilename());

                if (version != null) {
                    builder.setVersion(version);
                }
                if (blockList != null) {
                    builder.addAllBlocklist(blockList);
                }
            }

            FileInfo response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void modifyFile(FileInfo request, StreamObserver<WriteResult> responseObserver) {
            if (crashed) {
                responseObserver.onError(new RuntimeException("Server crashed!"));
                return;
            }

            WriteResult.Builder builder = WriteResult.newBuilder();
            String filename = request.getFilename();
            int cVersion = request.getVersion();
            int sVersion;

            if (!isLeader) {
                builder.setResult(WriteResult.Result.NOT_LEADER);
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
                return;
            }

            synchronized (this) {
                sVersion = versionMap.getOrDefault(filename, 0);
                // Check if client version == server version + 1
                if (cVersion != sVersion + 1) {
                    builder.setResult(WriteResult.Result.OLD_VERSION).setCurrentVersion(sVersion);
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                    return;
                }

                // Get missing blocks from block store
                List<String> missingBlocks = new ArrayList<>();
                for (String hash : request.getBlocklistList()) {
                    Block block = SurfStoreBasic.Block.newBuilder().setHash(hash).build();
                    if (!blockStub.hasBlock(block).getAnswer()) {
                        missingBlocks.add(hash);
                    }
                }

                // Client needs to store missing blocks to block store
                if (!missingBlocks.isEmpty()) {
                    builder.setResult(WriteResult.Result.MISSING_BLOCKS).setCurrentVersion(sVersion)
                            .addAllMissingBlocks(missingBlocks);
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                    return;
                }

                // Start two phase commit
                if (twoPhaseCommit(LogEntry.Command.MODIFY, request)) {
                    applyModify(request);
                    builder.setResult(WriteResult.Result.OK).setCurrentVersion(cVersion);
                } else {
                    builder.setResult(WriteResult.Result.ABORT).setCurrentVersion(sVersion);
                }

                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
            }
        }

        @Override
        public void deleteFile(FileInfo request, StreamObserver<WriteResult> responseObserver) {
            if (crashed) {
                responseObserver.onError(new RuntimeException("Server crashed!"));
                return;
            }

            WriteResult.Builder builder = WriteResult.newBuilder();
            String filename = request.getFilename();
            int cVersion = request.getVersion();
            int sVersion;

            if (!isLeader) {
                builder.setResult(WriteResult.Result.NOT_LEADER);
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
                return;
            }

            synchronized (this) {
                sVersion = versionMap.getOrDefault(filename, 0);
                if (sVersion == 0 || cVersion != sVersion + 1) {
                    builder.setResult(WriteResult.Result.OLD_VERSION).setCurrentVersion(sVersion);
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                    return;
                }

                // Start two phase commit
                if (twoPhaseCommit(LogEntry.Command.DELETE, request)) {
                    applyDelete(request);
                    builder.setResult(WriteResult.Result.OK).setCurrentVersion(cVersion);
                } else {
                    builder.setResult(WriteResult.Result.ABORT).setCurrentVersion(sVersion);
                }

                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
            }
        }

        private void applyModify(FileInfo request) {
            versionMap.put(request.getFilename(), request.getVersion());
            blockListMap.put(request.getFilename(), request.getBlocklistList());
            lastApplied++;
        }

        private void applyDelete(FileInfo request) {
            versionMap.put(request.getFilename(), request.getVersion());
            List<String> singleHashList = new ArrayList<>();
            singleHashList.add("0");
            blockListMap.put(request.getFilename(), singleHashList);
            lastApplied++;
        }

        private void applyEntries() {
            int lastIndex = logs.get(logs.size() - 1).getIndex();

            for (int i = lastApplied + 1; i <= lastIndex; i++) {
                LogEntry entry = logs.get(i);
                if (entry.getCommand() == LogEntry.Command.MODIFY) {
                    applyModify(entry.getRequest());
                } else if (entry.getCommand() == LogEntry.Command.DELETE) {
                    applyDelete(entry.getRequest());
                } else {
                    throw new IllegalStateException();
                }
            }

            lastApplied = lastIndex;
        }

        private boolean twoPhaseCommit(LogEntry.Command command, FileInfo request) {
            if (numMetaServer <= 1) {
                return true;
            }

            int commitIndex = logs.size();
            LogEntry entry = LogEntry.newBuilder().setIndex(commitIndex).setCommand(command).setRequest(request).build();
            logs.add(entry);

            // Prepare phase
            int counter = 0;
            ExecutorService executor = Executors.newFixedThreadPool(numMetaServer - 1);
            List<Future<SimpleAnswer>> results = new ArrayList<>();

            for (MetadataStoreGrpc.MetadataStoreBlockingStub follower : metaStubs) {
                results.add(executor.submit(() -> follower.prepare(entry)));
            }

            for (Future<SimpleAnswer> f : results) {
                try {
                    counter += f.get().getAnswer() ? 1 : 0;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            executor.shutdownNow();

            // Commit or abort phase, no need to send commit/abort to follower since
            // AppendEntries will send logs and apply anyway and resolve inconsistent logs
            if (counter < (numMetaServer + 1) / 2) {
                logs.remove(logs.size() - 1);
                return false;
            }

            return true;
        }

        @Override
        public void prepare(LogEntry request, StreamObserver<SimpleAnswer> responseObserver) {
            SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();

            if (crashed) {
                responseObserver.onNext(builder.setAnswer(false).build());
                responseObserver.onCompleted();
                return;
            }

            synchronized (this) {
                // Follower not recovered or has entries that should be removed due to missing abort
                if (request.getIndex() != logs.size()) {
                    responseObserver.onNext(builder.setAnswer(false).build());
                    responseObserver.onCompleted();
                } else {
                    logs.add(request);
                    responseObserver.onNext(builder.setAnswer(true).build());
                    responseObserver.onCompleted();
                }
            }
        }

        private void startAppendEntries() {
            daemonExecutor.scheduleAtFixedRate(this::sendAppendEntries, 500, 500, TimeUnit.MILLISECONDS);
        }

        private synchronized void sendAppendEntries() {
            ExecutorService executor = Executors.newFixedThreadPool(numMetaServer - 1);
            List<Future<LogIndex>> results = new ArrayList<>();

            for (int i = 0; i < numMetaServer - 1; i++) {
                List<LogEntry> entries = logs.subList(nextIndex[i], logs.size());
                LogEntries request = LogEntries.newBuilder().addAllEnries(entries).build();
                MetadataStoreGrpc.MetadataStoreBlockingStub metaStub = metaStubs[i];
                results.add(executor.submit(() -> metaStub.appendEntries(request)));
            }

            for (int i = 0; i < numMetaServer - 1; i++) {
                try {
                    nextIndex[i] = results.get(i).get().getIndex();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            executor.shutdownNow();
        }

        @Override
        public void appendEntries(LogEntries request, StreamObserver<LogIndex> responseObserver) {
            List<LogEntry> entries = request.getEnriesList();

            synchronized (this) {
                if (entries.isEmpty() || entries.get(0).getIndex() > logs.size()) {
                    responseObserver.onNext(LogIndex.newBuilder().setIndex(logs.size()).build());
                    responseObserver.onCompleted();
                    return;
                }

                int start = entries.get(0).getIndex();
                for (int i = logs.size() - 1; i >= start; i--) {
                    logs.remove(i);
                }
                logs.addAll(entries);

                applyEntries();

                responseObserver.onNext(LogIndex.newBuilder().setIndex(logs.size()).build());
                responseObserver.onCompleted();
            }
        }

        @Override
        public void isLeader(Empty request, StreamObserver<SimpleAnswer> responseObserver) {
            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(isLeader).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void crash(Empty request, StreamObserver<Empty> responseObserver) {
            crashed = true;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void restore(Empty request, StreamObserver<Empty> responseObserver) {
            crashed = false;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isCrashed(Empty request, StreamObserver<SimpleAnswer> responseObserver) {
            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(crashed).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getVersion(FileInfo request, StreamObserver<FileInfo> responseObserver) {
            // Getversion should always respond even when crashed
            FileInfo.Builder builder = FileInfo.newBuilder();
            builder.setFilename(request.getFilename());

            synchronized (this) {
                Integer version = versionMap.get(request.getFilename());

                if (version != null) {
                    builder.setVersion(version);
                }
            }

            FileInfo response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}