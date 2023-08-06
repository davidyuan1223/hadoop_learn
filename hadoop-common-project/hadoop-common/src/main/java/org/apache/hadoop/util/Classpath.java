package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.CommandFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@InterfaceAudience.Private
public class Classpath {
    private static final String usage=
            "classpath [--glob|--jar <path>|-h|--help] :\n"
                    + "  Prints the classpath needed to get the Hadoop jar and the required\n"
                    + "  libraries.\n"
                    + "  Options:\n"
                    + "\n"
                    + "  --glob       expand wildcards\n"
                    + "  --jar <path> write classpath as manifest in jar named <path>\n"
                    + "  -h, --help   print help\n";

    public static void main(String[] args) {
        if (args.length < 1 || args[0].equals("-h") || args[0].equals("--help")) {
            System.out.println(usage);
            return;
        }
        List<String > argList=new ArrayList<>(Arrays.asList(args));
        CommandFormat cf=new CommandFormat(0,Integer.MAX_VALUE,"-glob","-jar");
        try {
            cf.parse(argList);
        }catch (CommandFormat.UnknownOptionException e){
            terminate(1,"unrecognized option");
            return;
        }
        String classPath = System.getProperty("java.class.path");
        if (cf.getOpt("-glob")) {
            System.out.println(classPath);
        } else if (cf.getOpt("-jar")) {
            if (argList.isEmpty() || argList.get(0) == null || argList.get(0).isEmpty()) {
                terminate(1,"-jar option requires path of jar file to write");
                return;
            }
            Path workingDir = new Path(System.getProperty("user.dir"));
            final String tmpJarPath;
            try {
                tmpJarPath= FileUtil.createJarWithClassPath(classPath,workingDir,System.getenv())[0];
            }catch (IOException e){
                terminate(1,"I/O error creating jar: "+e.getMessage());
                return;
            }
            String jarPath=argList.get(0);
            try {
                FileUtil.replaceFile(new File(tmpJarPath),new File(jarPath));
            }catch (IOException e){
                terminate(1,"I/O error renaming jar temporary file to path: "+e.getMessage());
                return;
            }
        }
    }
    private static void terminate(int status,String msg){
        System.err.println(msg);
        ExitUtil.terminate(status,msg);
    }
}
