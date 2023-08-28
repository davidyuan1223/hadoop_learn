package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class GenericOptionParser {
    private static final Logger LOG= LoggerFactory.getLogger(GenericOptionParser.class);
    private Configuration conf;
    private CommandLine commandLine;
    private final boolean parseSuccessful;
    public GenericOptionParser(org.apache.commons.cli.Options opts, String[] args)throws IOException {
        this(new Configuration(),opts,args);
    }
    public GenericOptionParser(String[] args)throws IOException{
        this(new Configuration(),new org.apache.commons.cli.Options(),args);
    }
    public GenericOptionParser(Configuration conf,String[] args)throws IOException{
        this(conf,new org.apache.commons.cli.Options(),args);
    }
    public GenericOptionParser(Configuration conf, org.apache.commons.cli.Options options, String[] args)throws IOException{
        this.conf=conf;
        parseSuccessful=parseGeneralOptions(options,args);
    }
    public String[] getRemainingArgs(){
        return commandLine==null?new String[]{}:commandLine.getArgs();
    }
    public Configuration getConfiguration(){
        return conf;
    }

    public CommandLine getCommandLine() {
        return commandLine;
    }

    public boolean isParseSuccessful() {
        return parseSuccessful;
    }
    @SuppressWarnings("static-access")
    protected org.apache.commons.cli.Options buildGeneralOptions(org.apache.commons.cli.Options opts){
        synchronized (Option.class){
            Option fs=Option.builder("fs")
                    .argName("file:///|hdfs://namenode:port")
                    .hasArg()
                    .desc("specify default filesystem URL to use," +
                            "overrides 'fs.defaultFS' property from configuration.")
                    .build();
            Option jt = Option.builder("jt").argName("local|resourcemanager:port")
                    .hasArg()
                    .desc("specify a ResourceManager")
                    .build();
            Option oconf = Option.builder("conf")
                    .argName("configuration file")
                    .hasArg()
                    .desc("specify an application cofiguration file")
                    .build();
            Option property = Option.builder("D")
                    .argName("property=value")
                    .hasArg()
                    .desc("use value for given property")
                    .build();
            Option libjars = Option.builder("libjars")
                    .argName("paths")
                    .hasArg()
                    .desc("comma separated jar files to include in classpath.")
                    .build();
            Option files = Option.builder("files")
                    .argName("paths")
                    .hasArg()
                    .desc("comma separated files to be copied to the map reduce cluster")
                    .build();
            Option archives = Option.builder("archives").argName("paths")
                    .hasArg()
                    .desc("comma separated archives to be unarchived" +
                            " on the compute machines.")
                    .build();

            // file with security tokens
            Option tokensFile = Option.builder("tokenCacheFile").argName("tokensFile")
                    .hasArg()
                    .desc("name of the file with the tokens")
                    .build();


            opts.addOption(fs);
            opts.addOption(jt);
            opts.addOption(oconf);
            opts.addOption(property);
            opts.addOption(libjars);
            opts.addOption(files);
            opts.addOption(archives);
            opts.addOption(tokensFile);

            return opts;
        }
    }
    private void processGeneralOptions(CommandLine line)throws IOException{
        if (line.hasOption("fs")) {
            FileSystem.setDefaultUri(conf,line.getOptionValue("fs"));
        }
        if (line.hasOption("jt")) {
            String optionValue = line.getOptionValue("jt");
            if (optionValue.equalsIgnoreCase("local")) {
                conf.set("mapreduce.framework.name",optionValue);
            }
            conf.set("yarn.resourcemanager.address",optionValue,
                    "from -jt command line option");
        }
        if (line.hasOption("conf")) {
            String[] values = line.getOptionValues("conf");
            for (String value : values) {
                conf.addResource(new Path(value));
            }
        }
        if (line.hasOption("D")) {
            String[] property = line.getOptionValues("D");
            for (String prop : property) {
                String[] keyval = prop.split("=", 2);
                if (keyval.length == 2) {
                    conf.set(keyval[0], keyval[1],"from command line");
                }
            }
        }
        if (line.hasOption("libjars")) {
            conf.set("tmpjars",
                    validateFiles(line.getOptionValue("libjars")));
        }
    }
}
