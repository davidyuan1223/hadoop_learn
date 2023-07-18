package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Shell {
    private static final Map<Shell,Object> CHILD_SHELLS= Collections.synchronizedMap(new WeakHashMap<>());
    public static final Logger LOG= LoggerFactory.getLogger(Shell.class);
    private static final String WINDOWS_PROBLEMS= "https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems";
    static final String WINUTILS_EXE="winutils.exe";
    public static final String SYSPROP_HADOOP_HOME_DIR="hadoop.home.dir";
    public static final String ENV_HAOOP_HOME="HADOOP_HOME";
    private static final int JAVA_SPEC_VAR=Math.max(8,Integer.parseInt(
            System.getProperty("java.specification.version")
            .split("\\.")
            [0]
    ));
    public static final int WINDOWS_MAX_SHELL_LENGTH=8191;
    @Deprecated
    public static final int WINDOWS_MAX_SHELL_LENGHT=WINDOWS_MAX_SHELL_LENGTH;
    public static final String USER_NAME_COMMAND="whoami";
    public static final Object WindowsProcessLaunchLock=new Object();
    public enum OSType{
        OS_TYPE_LINUX,
        OS_TYPE_WIN,
        OS_TYPE_SOLARIS,
        OS_TYPE_MAC,
        OS_TYPE_FREEBSD,
        OS_TYPE_OTHER
    }

    public static final OSType osType=getOSType();
    public static final boolean WINDOWS=(osType==OSType.OS_TYPE_WIN);
    public static final boolean SOLARIS=(osType==OSType.OS_TYPE_SOLARIS);
    public static final boolean MAC=(osType==OSType.OS_TYPE_MAC);
    public static final boolean FREEBSD=(osType==OSType.OS_TYPE_FREEBSD);
    public static final boolean LINUX=(osType==OSType.OS_TYPE_LINUX);
    public static final boolean OTHER=(osType==OSType.OS_TYPE_OTHER);

    public static final boolean PPC_64=System.getProperties().getProperty("os.arch").contains("ppc64");
    public static final String ENV_NAME_REGEX="[A-Za-z_][A-Za-z0-9_]*";
    public static final String SET_PERMISSION_COMMAND="chmod";
    public static final String SET_OWNER_COMMAND="chown";
    public static final String SET_GROUP_COMMAND="chgrp";
    public static final String LINK_COMMAND="ln";
    public static final String READ_LINK_COMMAND="readlink";
    protected long timeOutInterval=0L;
    private final AtomicBoolean timeOut=new AtomicBoolean(false);
    protected boolean inheritParentEnv=true;
    static final String E_DOSE_NOT_EXIST="does not exist";
    static final String E_IS_RELATIVE="is not an absolute path.";
    static final String E_NOT_DIRECTORY="is not a directory";
    static final String E_NO_EXECUTABLE="Could not locate Hadoop executable";
    static final String E_NOT_EXECUTABLE_FILE="Not an executable file";
    static final String E_HADOOP_PROPS_UNSET=ENV_HAOOP_HOME+" and "+SYSPROP_HADOOP_HOME_DIR+" are unset.";
    static final String E_HADOOP_PROPS_EMPTY=ENV_HAOOP_HOME+" or "+SYSPROP_HADOOP_HOME_DIR+" set to an empty string";
    static final String E_NOT_A_WINDOWS_SYSTEM="Not a Windows system";
    private static final File HADOOP_HOME_FILE;
    private static final IOException HADOOP_HOME_DIR_FAILURE_CAUSE;
    @Deprecated
    public static final String WINUTILS;
    private static final String WINUTILS_PATH;
    private static final File WINUTILS_FILE;
    private static final IOException WINUTILS_FAILURE;
    public static final boolean isSetsidAvailable=isSetsidSupported();
    public static final String TOKEN_SEPARATOR_REGEX=WINDOWS?"[|\n\r]":"[ \t\n\r\f]";
    private long interval;
    private long lastTime;
    private final boolean redirectErrorStream;
    private Map<String ,String> environment;
    private File dir;
    private Process process;
    private int exitCode;
    private Thread waitingThread;
    private final AtomicBoolean completed=new AtomicBoolean(false);

    static {
        File home;
        IOException ex;
        try {
            home=checkHadoopHome();
            ex=null;
        }catch (IOException e){
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to detect a valid hadoop home directory",e);
            }
            ex=e;
            home=null;
        }
        HADOOP_HOME_FILE=home;
        HADOOP_HOME_DIR_FAILURE_CAUSE=ex;
    }
    static {
        IOException ioe=null;
        String path=null;
        File file=null;
        if (WINDOWS) {
            try {
                file=getQualifiedBin(WINUTILS_EXE);
                path=file.getCanonicalPath();
                ioe=null;
            }catch (IOException e){
               LOG.warn("Dit not find {}: {}",WINUTILS_EXE,e);
               LOG.debug("Failed to find "+WINUTILS_EXE,e);
               file=null;
               path=null;
               ioe=e;
            }
        }else {
            ioe=new FileNotFoundException(E_NOT_A_WINDOWS_SYSTEM);
        }
        WINUTILS_PATH=path;
        WINUTILS_FILE=file;
        WINUTILS=path;
        WINUTILS_FAILURE=ioe;
    }

    protected Shell(){
        this(0L);
    }
    protected Shell(long interval){
        this(interval,false);
    }
    protected Shell(long interval,boolean redirectErrorStream){
        this.interval=interval;
        this.lastTime=(interval<0)?0:-interval;
        this.redirectErrorStream=redirectErrorStream;
        this.environment=Collections.emptyMap();
    }
    @Deprecated
    public static boolean isJava7OrAbove(){return true;}
    public static boolean isJavaVersionAtLeast(int version){
        return JAVA_SPEC_VAR>=version;
    }
    public static void checkWindowsCommandLineLength(String ...commands) throws IOException {
        int len=0;
        for (String command : commands) {
            len+=command.length();
        }
        if (len > WINDOWS_MAX_SHELL_LENGTH) {
            throw new IOException(String.format(
                    "The command line has a length of %d exceed maximum allowed length of %d. Command starts with: %s",
                    len,WINDOWS_MAX_SHELL_LENGTH,StringUtils.join("",commands).substring(0,100)
            ));
        }
    }
    static String bashQueue(String arg){
        StringBuilder sb = new StringBuilder(arg.length() + 2);
        sb.append('\'')
                .append(arg.replace("'","'\\''"))
                .append('\'');
        return sb.toString();
    }
    private static OSType getOsType(){
        String osName = System.getProperty("os.name");
        if (osName.startsWith("Windows")) {
            return OSType.OS_TYPE_WIN;
        } else if (osName.contains("SunOS") || osName.contains("Solaris")) {
            return OSType.OS_TYPE_SOLARIS;
        } else if (osName.contains("Mac")) {
            return OSType.OS_TYPE_MAC;
        } else if (osName.contains("FreeBSD")) {
            return OSType.OS_TYPE_FREEBSD;
        } else if (osName.contains("Linux")) {
            return OSType.OS_TYPE_LINUX;
        }else {
            return OSType.OS_TYPE_OTHER;
        }
    }
    public static String[] getGroupsCommand(){
        return (WINDOWS)?new String[]{"cmd","/c","groups"}:new String[]{"groups"};
    }
    public static String[] getGroupsForUserCommand(final String user){
        if (WINDOWS) {
            return new String[]{
                    getWinUtilsPath(),"groups","-F","\""+user+"\""
            };
        }else {
            String quoteUser=baseQuote(user);
            return new String[]{"bash","-c","id -gn "+quoteUser+"; id -Gn "+quoteUser};
        }
    }
    public static String[] getGroupsForUserCommand(final String user){
        if (WINDOWS) {
            return new String[]{
                    getWinUtilsPath(),"groups","-F","\""+user+"\""
            };
        }else {
            String quoteUser=baseQuote(user);
            return new String[]{"bash","-c","id -g "+quoteUser+"; id -G "+quoteUser};
        }
    }
    public static String[] getUsersForNetgroupCommand(final String netgroup){
        return new String[]{"getent","netgroup",netgroup};
    }
    public static String[] getGetPermissionCommand(){
        return (WINDOWS)?new String[]{getWinUtilsPath(),"ls","-F"}
                :new String[]{"ls","-ld"};
    }
    public static String[] getSetPermissionCommand(String perm,boolean recursive){
        if (recursive) {
            return WINDOWS?
                    new String[]{getWinUtilsPath(),"chmod","-R",perm}
                    :new String[]{"chmod","-R",perm};
        }else {
            return WINDOWS?
                    new String[]{getWinUtilsPath(),"chmod",perm}
                    :new String[]{"chmod",perm};
        }
    }
    public static String[] getSetPermissionCommand(String perm,boolean recursive,String file){
        String[] bashCmd = getSetPermissionCommand(perm, recursive);
        String[] cmdWithFile = Arrays.copyOf(bashCmd, bashCmd.length + 1);
        cmdWithFile[cmdWithFile.length-1]=file;
        return cmdWithFile;
    }
    public static String[] getSetOwnerCommand(String owner){
        return WINDOWS?
                new String[]{getWinUtilsPath(),"chown","\""+owner+"\""}
                :new String[]{"chown",owner};
    }
    public static String[] getSymlinkCommand(String target,String link){
        return WINDOWS?
                new String[]{getWinUtilsPath(),"symlink",link,target}
                :new String[]{"ln","-s",target,link};
    }
    public static String[] getReadlinkCommand(String link){
        return WINDOWS?
                new String[]{getWinUtilsPath(),"readlink",link}
                :new String[]{"readlink",link};
    }
    public static String[] getCheckProcessIsAliveCommand(String pid){
        return getSignalKillCommand(0,pid);
    }

    public static String[] getSignalKillCommand(int code,String pid){
        if (Shell.WINDOWS) {
            if (0 == code) {
                return new String[]{Shell.getWinUtilsPath(),"task","isAlive",pid};
            }else {
                return new String[]{Shell.getWinUtilsPath(),"task","kill",pid};
            }
        }
        final String quotedPid=baseQuote(pid);
        if (isSetsidAvailable) {
            return new String[]{"bash","-c","kill -"+code+" -- -"+quotedPid};
        }else {
            return new String[]{"bash","-c","kill -"+code+" "+quotedPid};
        }
    }
    public static String getEnvironmentVariableRegex(){
        return WINDOWS?
                "%("+ENV_NAME_REGEX+"?)%"
                :"\\$("+ENV_NAME_REGEX+")";
    }
    public static File appendScriptExtension(File parent,String basename){
        return new File(parent,appendScriptExtension(basename));
    }
    public static String appendScriptExtension(String basename){
        return basename+(WINDOWS?".cmd":".sh");
    }
    public static String[] getRunScriptCommand(File script){
        String absolutePath = script.getAbsolutePath();
        return WINDOWS?
                new String[]{"cmd","/c",absolutePath}
                :new String[]{"bash",bashQuote(absolutePath);}
    }


    public static String getWinUtilsPath() {
        if (WINUTILS_FAILURE == null) {
            return WINUTILS_PATH;
        }else {
            throw new RuntimeException(WINUTILS_FAILURE.toString(),WINUTILS_FAILURE);
        }
    }


    public static File getQualifiedBin(String executable) throws FileNotFoundException {
        return getQualifiedBinInner(getHadoopHomeDir(),executable);
    }

    static File getQualifiedBinInner(File hadoopHomeDir, String executable) throws FileNotFoundException {
        String binDirText="Hadoop bin directory";
        File bin=new File(hadoopHomeDir,"bin");
        if (!bin.exists()) {
            throw new FileNotFoundException(addOsText(binDirText+E_DOSE_NOT_EXIST+" : "+bin));
        }
        if (!bin.isDirectory()) {
            throw new FileNotFoundException(addOsText(binDirText+E_NOT_DIRECTORY+" : "+bin));
        }
        File execFile=new File(bin,executable);
        if (!execFile.exists()) {
            throw new FileNotFoundException(addOsText(E_NO_EXECUTABLE+ " : "+execFile));
        }
        if (!execFile.isFile()){
            throw new FileNotFoundException(addOsText(E_NOT_EXECUTABLE_FILE+" : "+execFile));
        }
        try {
            return execFile.getCanonicalFile();
        } catch (IOException e) {
            throw fileNotFoundException(e.toString(),e);
        }
    }

    private static File getHadoopHomeDir() throws FileNotFoundException {
        if (HADOOP_HOME_DIR_FAILURE_CAUSE != null) {
            throw fileNotFoundException(
                    addOsText(HADOOP_HOME_DIR_FAILURE_CAUSE.toString()),
                    HADOOP_HOME_DIR_FAILURE_CAUSE
            );
        }
        return HADOOP_HOME_FILE;
    }

    private static FileNotFoundException fileNotFoundException(String text, Exception ex) {
        return (FileNotFoundException) new FileNotFoundException(text).initCause(ex);
    }

    private static String addOsText(String message) {
        return WINDOWS?(message+" -see "+WINDOWS_PROBLEMS):message;
    }


    private static File checkHadoopHome()throws FileNotFoundException {
        String home=System.getProperty(SYSPROP_HADOOP_HOME_DIR);
        if (home == null) {
            home=System.getenv(ENV_HAOOP_HOME);
        }
        return checkHadoopHomeInner(home);
    }
    @VisibleForTesting
    private static File checkHadoopHomeInner(String home) throws FileNotFoundException {
        if (home == null) {
            throw new FileNotFoundException(E_HADOOP_PROPS_UNSET);
        }
        while (home.startsWith("\"")) {
            home=home.substring(1);
        }
        while (home.endsWith("\"")) {
            home=home.substring(0,home.length()-1);
        }
        if (home.isEmpty()) {
            throw new FileNotFoundException(E_HADOOP_PROPS_EMPTY);
        }
        File homeDir=new File(home);
        if (!homeDir.isAbsolute()) {
            throw new FileNotFoundException("Hadoop home directory "+homeDir+" "+E_IS_RELATIVE);
        }
        if (!homeDir.exists()) {
            throw new FileNotFoundException("Hadoop home directory "+homeDir+" "+E_DOSE_NOT_EXIST);
        }
        if (!homeDir.isDirectory()) {
            throw new FileNotFoundException("Hadoop home directory "+homeDir+" "+E_NOT_DIRECTORY);
        }
        return homeDir;
    }

    private static boolean isSetsidSupported() {
    }


    private static OSType getOSType(){

    }


    public static class ExitCodeException extends IOException{
        private final int exitCode;
        public ExitCodeException(int exitCode,String message){
            super(message);
            this.exitCode=exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }

        @Override
        public String toString() {
            final StringBuilder sb=new StringBuilder("ExitCodeException ");
            sb.append("exitCode=")
                    .append(exitCode)
                    .append(": ")
                    .append(super.getMessage());
            return sb.toString();
        }
    }
    public interface CommandExecutor{
        void execute() throws IOException;

        int getExitCode() throws IOException;

        String getOutput() throws IOException;

        void close();
    }

    public static class ShellCommandExecutor extends Shell implements CommandExecutor{
        private String[] command;
        private StringBuffer output;

        public ShellCommandExecutor(String[] execString){this(execString,null);}
        public ShellCommandExecutor(String[] execString, File dir){this(execString,dir,null);}
        public ShellCommandExecutor(String[] execString,File dir,Map<String ,String > env){
            this(execString,dir,env,0L);
        }
        public ShellCommandExecutor(String[] execString,File dir,Map<String ,String > env,long timeout){
            this(execString,dir,env,timeout,true);
        }
        public ShellCommandExecutor(String[] execString,File dir,Map<String ,String > env,long timeout,boolean inheritParentEnv){
            command=execString.clone();
            if (dir != null) {
                setWorkingDirectory(dir);
            }
            if (env != null) {
                setEnvironment(env);
            }
            timeOutInterval=timeout;
            this.inheritParentEnv=inheritParentEnv;
        }


        @Override
        public void execute() throws IOException {

        }

        @Override
        public int getExitCode() throws IOException {
            return 0;
        }

        @Override
        public String getOutput() throws IOException {
            return null;
        }

        @Override
        public void close() {

        }
    }
}
