package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;


public final class BlockStore {
    private static final Logger logger = Logger.getLogger(BlockStore.class.getName());

    protected Server server;
	protected ConfigReader config;

    public BlockStore(ConfigReader config) {
    	this.config = config;
	}

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new BlockStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                BlockStore.this.stop();
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
        ArgumentParser parser = ArgumentParsers.newFor("BlockStore").build()
                .description("BlockStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
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

        final BlockStore server = new BlockStore(config);
        server.start(config.getBlockPort(), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class BlockStoreImpl extends BlockStoreGrpc.BlockStoreImplBase {
        private Map<String, byte[]> blockMap;

        BlockStoreImpl() {
            this.blockMap = new HashMap<>();
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void storeBlock(SurfStoreBasic.Block request, StreamObserver<Empty> responseObserver) {
            synchronized (this) {
                blockMap.put(request.getHash(), request.getData().toByteArray());
            }
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getBlock(SurfStoreBasic.Block request, StreamObserver<SurfStoreBasic.Block> responseObserver) {
            SurfStoreBasic.Block.Builder builder = SurfStoreBasic.Block.newBuilder();
            builder.setHash(request.getHash());

            synchronized (this) {
                byte[] data = blockMap.get(request.getHash());
                if (data != null) {
                    builder.setData(ByteString.copyFrom(data));
                }
            }

            SurfStoreBasic.Block response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void hasBlock(SurfStoreBasic.Block request, StreamObserver<SurfStoreBasic.SimpleAnswer> responseObserver) {
            SurfStoreBasic.SimpleAnswer.Builder builder = SurfStoreBasic.SimpleAnswer.newBuilder();

            synchronized (this) {
                boolean answer = blockMap.containsKey(request.getHash());
                builder.setAnswer(answer);
            }

            SurfStoreBasic.SimpleAnswer response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}