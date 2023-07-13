package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.shell.CommandFormat;

import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.Private
public final class Classpath {
    private static final String usage=
            "classpath [--glob|--jar <path>|-h|--help] :\n"
            +"  Prints the classpath needed to get the Hadoop jar and the required\n"
            +"  libraries.\n"
            +"  Options:\n"
            +"\n"
            +"  --glob     expand wildcards\n"
            +"  --jar <path> write classpath as manifest in jar named <path>\n"
            +"  -h, --help print help\n";

    public static void main(String[] args) {
        if (args.length < 1 || args[0].equals("-h") || args[0].equals("--help")) {
            System.out.println(usage);
            return;
        }
        // copy args,because CommandFormat mutates the list
        List<String > argList=new ArrayList<>();
        CommandFormat commandFormat = new CommandFormat(0, Integer.MAX_VALUE, "-glob", "-jar");
        try {
            commandFormat.parse(argList);
        }catch (CommandFormat.UnknownOptionException e){
            terminate(1,"unrecognized option");
            return;
        }
    }

    private static void terminate(int status, String msg) {
        System.err.println(msg);
        ExitUtil.terminate(status,msg);
    }
}
