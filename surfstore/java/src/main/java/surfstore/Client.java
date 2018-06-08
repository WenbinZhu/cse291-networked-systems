package surfstore;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;


public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel leaderChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub leaderStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    public Client(ConfigReader config) {
        int leader = config.getLeaderNum();
        this.leaderChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(leader))
                .usePlaintext(true).build();
        this.leaderStub = MetadataStoreGrpc.newBlockingStub(leaderChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
    }

    public void shutdown() throws InterruptedException {
        leaderChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private Block stringToBlock(String s) {
        Block.Builder builder = Block.newBuilder();

        builder.setHash(BlockUtil.sha256(s));
        builder.setData(ByteString.copyFrom(s, StandardCharsets.UTF_8));

        return builder.build();
    }

	private void go(Namespace c_args) {
        String command = c_args.getString("command");
        String filename = c_args.getString("target_file");
        String directory = c_args.getString("download_dir");

        if (command.equals("upload")) {
            upload(filename);
        } else if (command.equals("download")) {
            download(filename, directory);
        } else if (command.equals("delete")) {
            delete(filename);
        } else if (command.equals("getversion")) {
            getVersion(filename);
        } else {
            throw new IllegalArgumentException("command not supported");
        }
	}

	private synchronized void upload(String filepath) {
        String[] path = filepath.trim().split("/");
        String filename = path[path.length - 1];
        FileInfo.Builder builder = FileInfo.newBuilder().setFilename(filename);

        List<String> dataBlocks = BlockUtil.readBlocksFromFile(filepath);
        if (dataBlocks == null) {
            System.out.println("Not Found");
            return;
        }

        int version = leaderStub.getVersion(builder.build()).getVersion();
        builder.setVersion(++version);

        Map<String, Block> blockMap = new HashMap<>();
        for (String d : dataBlocks) {
            Block block = stringToBlock(d);
            builder.addBlocklist(block.getHash());
            blockMap.put(block.getHash(), block);
        }

        while (true) {
            WriteResult response = leaderStub.modifyFile(builder.build());
            if (response.getResult() == WriteResult.Result.NOT_LEADER) {
                System.out.println("Not Leader");
                return;
            }
            while (response.getResult() == WriteResult.Result.OLD_VERSION) {
                builder.setVersion(++version);
                response = leaderStub.modifyFile(builder.build());
            }
            if (response.getResult() == WriteResult.Result.MISSING_BLOCKS) {
                for (String hash : response.getMissingBlocksList()) {
                    blockStub.storeBlock(blockMap.get(hash));
                }
            }
            if (response.getResult() == WriteResult.Result.OK) {
                System.out.println("OK");
                return;
            }
        }
    }

    private synchronized void download(String filename, String dir) {
        FileInfo request = FileInfo.newBuilder().setFilename(filename).build();
        FileInfo response = leaderStub.readFile(request);

        Map<String, byte[]> hashBlockMap = BlockUtil.scanBlocksInDir(dir);
        if (hashBlockMap == null) {
            System.out.println("Not Found");
            return;
        }

        if (response.getVersion() == 0 || response.getBlocklistList().isEmpty() ||
            response.getBlocklist(0).equals("0")) {
            System.out.println("Not Found");
            return;
        }

        List<byte[]> blocksToWrite = new ArrayList<>();
        for (String hash : response.getBlocklistList()) {
            if (hashBlockMap.containsKey(hash)) {
                blocksToWrite.add(hashBlockMap.get(hash));
            } else {
                Block block = blockStub.getBlock(Block.newBuilder().setHash(hash).build());
                blocksToWrite.add(block.getData().toByteArray());
            }
        }

        if (BlockUtil.writeBlocksToFile(filename, dir, blocksToWrite)) {
            System.out.println("OK");
        } else {
            System.out.println("Not Found");
        }
    }

    private synchronized void delete(String filename) {
        FileInfo.Builder builder = FileInfo.newBuilder().setFilename(filename);
        int version = leaderStub.getVersion(builder.build()).getVersion();

        if (version == 0) {
            System.out.println("Not Found");
            return;
        }

        builder.setVersion(++version);
        WriteResult response = leaderStub.deleteFile(builder.build());

        if (response.getResult() == WriteResult.Result.NOT_LEADER) {
            System.out.println("Not Leader");
            return;
        }
        while (response.getResult() == WriteResult.Result.OLD_VERSION) {
            builder.setVersion(++version);
            response = leaderStub.deleteFile(builder.build());
        }

        System.out.println("OK");
    }

    private synchronized void getVersion(String filename) {
        FileInfo request = FileInfo.newBuilder().setFilename(filename).build();
        System.out.println(leaderStub.getVersion(request).getVersion());
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("command").type(String.class)
                .choices("upload", "download", "delete", "getversion")
                .help("Client command, upload/download/delete/getversion");
        parser.addArgument("target_file").type(String.class)
                .help("Path of file to upload or filename to download/delete/getversion");

        if (args[1].equals("download")) {
            parser.addArgument("download_dir").type(String.class)
                    .help("Path of download directory");
        }
        
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

        Client client = new Client(config);
        
        try {
        	client.go(c_args);
        } finally {
            client.shutdown();
        }
    }
}
