package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
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
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.WriteResult;


public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;

    public MetadataStore(ConfigReader config) {
    	this.config = config;
	}

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(config))
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

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
        private boolean isLeader;
        private boolean crashed;
        private Map<String, Integer> versionMap;
        private Map<String, List<String>> blockListMap;

        private BlockStoreGrpc.BlockStoreBlockingStub blockStub;

        MetadataStoreImpl(ConfigReader config) {
            this.isLeader = true;
            this.versionMap = new HashMap<>();
            this.blockListMap = new HashMap<>();

            ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1",
                    config.getBlockPort()).usePlaintext(true).build();
            this.blockStub = BlockStoreGrpc.newBlockingStub(channel);
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void readFile(SurfStoreBasic.FileInfo request, StreamObserver<SurfStoreBasic.FileInfo> responseObserver) {
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
        public void modifyFile(SurfStoreBasic.FileInfo request, StreamObserver<SurfStoreBasic.WriteResult> responseObserver) {
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
                if (cVersion != sVersion + 1) {
                    builder.setResult(WriteResult.Result.OLD_VERSION).setCurrentVersion(sVersion);
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                    return;
                }

                List<String> missingBlocks = new ArrayList<>();
                for (String hash : request.getBlocklistList()) {
                    Block block = SurfStoreBasic.Block.newBuilder().setHash(hash).build();
                    if (!blockStub.hasBlock(block).getAnswer()) {
                        missingBlocks.add(hash);
                    }
                }

                if (!missingBlocks.isEmpty()) {
                    builder.setResult(WriteResult.Result.MISSING_BLOCKS).setCurrentVersion(sVersion)
                            .addAllMissingBlocks(missingBlocks);
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                    return;
                }

                versionMap.put(filename, cVersion);
                blockListMap.put(filename, request.getBlocklistList());

                builder.setResult(WriteResult.Result.OK).setCurrentVersion(cVersion);
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
            }
        }

        @Override
        public void deleteFile(SurfStoreBasic.FileInfo request, StreamObserver<SurfStoreBasic.WriteResult> responseObserver) {
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

                versionMap.put(filename, cVersion);
                List<String> singleHashList = new ArrayList<>();
                singleHashList.add("0");
                blockListMap.put(filename, singleHashList);

                builder.setResult(WriteResult.Result.OK).setCurrentVersion(cVersion);
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
            }
        }

        @Override
        public void isLeader(Empty request, StreamObserver<SurfStoreBasic.SimpleAnswer> responseObserver) {
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
        public void isCrashed(Empty request, StreamObserver<SurfStoreBasic.SimpleAnswer> responseObserver) {
            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(crashed).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getVersion(SurfStoreBasic.FileInfo request, StreamObserver<SurfStoreBasic.FileInfo> responseObserver) {
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