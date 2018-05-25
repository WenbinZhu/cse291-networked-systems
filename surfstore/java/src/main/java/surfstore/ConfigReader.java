package surfstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ConfigReader {
    private static final String numMetadataMatchStr = "M(:|=)\\s*(?<numMetadata>\\d+)";
    private static final String leaderNumMatchStr = "L(:|=)\\s*(?<leaderNum>\\d+)";
    private static final String metadataInstMatchStr = "metadata(?<metadataId>\\d+)(:|=)\\s*(?<metadataPort>\\d+)";
    private static final String blockInstMatchStr = "block(:|=)\\s*(?<blockPort>\\d+)";
    
    private static final Pattern configMatcher = Pattern.compile(
            String.format("((%s)|(%s)|(%s)|(%s))\\s*",
                numMetadataMatchStr,
                metadataInstMatchStr,
                blockInstMatchStr,
                leaderNumMatchStr
            ));

    protected File configFile;
    protected String config;
    
    public Integer numMetadataServers;
    public HashMap<Integer, Integer> metadataPorts;
    public Integer blockPort;
    public Integer leaderNum;
    
	public ConfigReader(File configFile) throws FileNotFoundException {
		if (!configFile.exists()) {
			throw new FileNotFoundException(configFile.getPath());
		}
		
		try {
			config = new String(Files.readAllBytes(configFile.toPath()), "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		parseConfigFile();
	}
	
	public ConfigReader(String configString) {
		configFile = null;
		config = configString;
		parseConfigFile();
	}

	protected void parseConfigFile() {
        metadataPorts = new HashMap<Integer, Integer>();

        for(String line : config.split("\\r?\\n")) {
            Matcher result = configMatcher.matcher(line);
            if(!result.matches()) {
                System.err.println("ConfigReader: Invalid line:\n" + line);
            }
            else if (result.group("numMetadata") != null) {
                numMetadataServers = Integer.parseInt(result.group("numMetadata"));
            } else if (result.group("leaderNum") != null) {
            	leaderNum = Integer.parseInt(result.group("leaderNum"));
            } else if (result.group("metadataId") != null) {
                metadataPorts.put(Integer.parseInt(result.group("metadataId")),
                                  Integer.parseInt(result.group("metadataPort")));
            } else if (result.group("blockPort") != null) {
                blockPort = Integer.parseInt(result.group("blockPort"));
            } else{
                System.err.println("ConfigReader: Invalid line:\n" + line);
            }
        }

        if (numMetadataServers == null || blockPort == null || leaderNum == null) {
            throw new RuntimeException("Config file is missing one or more required lines!");
        }

        for(int i = 1; i <= numMetadataServers; i++) {
            if (!metadataPorts.containsKey(i))
                throw new RuntimeException("Must set port for metadata" + i);
        }
    }

    public int getNumMetadataServers() {
        return numMetadataServers;
    }

    public int getMetadataPort(int serverId) {
        return metadataPorts.get(serverId);
    }

    public int getBlockPort() {
        return blockPort;
    }
    
    public int getLeaderNum() {
    	return leaderNum;
    }
}