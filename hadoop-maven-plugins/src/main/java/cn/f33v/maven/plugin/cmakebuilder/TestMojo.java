package cn.f33v.maven.plugin.cmakebuilder;

import cn.f33v.maven.plugin.util.Exec;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Description: Goal which runs a native unit test
 * @Author: yuan
 * @Date: 2023/07/11
 **/
@Mojo(name = "cmake-test",defaultPhase = LifecyclePhase.TEST)
public class TestMojo extends AbstractMojo {
    /**
     * A value for -Dtest= that runs all native tests
     */
    private static final String ALL_NATIVE="allNative";
    /**
     * location of the binary to run
     */
    @Parameter(required = true)
    private File binary;
    /**
     * Name for this test.
     * Defaults to the basename of the binary.So if your binary is /foo/bar/baz,
     * this will default to baz
     */
    @Parameter
    private String testName;
    /**
     * Environment variable to pass to the binary
     */
    @Parameter
    private Map<String ,String > env;
    /**
     * Arguments to pass to the binary
     */
    @Parameter
    private List<String > args=new ArrayList<>();
    /**
     * Number of seconds to wait before declaring the test failed
     */
    @Parameter(defaultValue = "600")
    private int timeout;
    /**
     * The working directory to use
     */
    @Parameter
    private File workingDirectory;
    /**
     * Path to results directory
     */
    @Parameter(defaultValue = "native-results")
    private File results;
    /**
     * A list of preconditions which must be true for this test to be run
     */
    @Parameter
    private Map<String ,String > preConditions=new HashMap<>();
    /**
     * If true,pass over the test without an error if the binary is missing
     */
    @Parameter(defaultValue = "false")
    private boolean skipIfMissing;
    /**
     * What result to expect from the test
     * Can be either "success","failure","any"
     */
    @Parameter(defaultValue = "success")
    private String expectedResult;
    /**
     * The Maven Session Object
     */
    @Parameter(defaultValue = "${session}",readonly = true,required = true)
    private MavenSession session;
    //TODO: support Windows
    private static void validatePlatform()throws MojoExecutionException{
        if (System.getProperty("os.name").toLowerCase(Locale.ENGLISH).startsWith("windows")) {
            throw new MojoExecutionException("CMakeBuilder does not yet support the Windows platform.");
        }
    }
    /**
     * The test thread waits for the process to terminate
     * Since Process#witFor doesn't take timout argument.we simulate one by interrupting
     * this thread after a certain amount of time has elapsed
     */
    private static class TestThread extends Thread{
        private Process process;
        private int retCode=-1;
        public TestThread(Process process){this.process=process;}

        @Override
        public void run() {
            try {
                retCode=process.waitFor();
            }catch (InterruptedException e){
                retCode=-1;
            }
        }
        public int retCode(){return retCode;}
    }
    /**
     * write to the status file
     * the status file will contain a string describing the exit status of the test.
     * it will be SUCCESS if the test returned success(return code 0),a numerical code if it
     * returned a non-zero status,or IN_PROGRESS or TIME_OUT
     */
    private void writeStatusFile(String status)throws IOException{
        FileOutputStream fos=new FileOutputStream(new File(results,testName+".pstatus"));
        BufferedWriter out=null;
        try {
            out=new BufferedWriter(new OutputStreamWriter(fos,"UTF8"));
            out.write(status+"\n");
        }finally {
            if (out != null) {
                out.close();
            }else {
                fos.close();
            }
        }
    }
    private static boolean isTruthy(String str){
        switch (str){
            case "":
            case "false":
            case "no":
            case "off":
            case "disable":
              return false;
        }
        return true;
    }

    final static private String VALID_PRECONDITION_TYPES_STR="Valid precondition types are \"and\",\"andNot\"";
    /**
     * Validate the parameters that user has passed
     */
    private void validateParameters()throws MojoExecutionException{
        if (!(expectedResult.equals("success")
        ||expectedResult.equals("failure")
        ||expectedResult.equals("any"))){
            throw new MojoExecutionException("expectedResult must be either success,failure,any");
        }
    }
    private boolean shouldRunTest()throws MojoExecutionException{
        //Were we told to skip all tests?
        String skipTests = session.getSystemProperties().getProperty("skipTests");
        if (isTruthy(skipTests)) {
            getLog().info("skipTests is in effect for test "+testName);
            return false;
        }
        // Does the binary exists? If not,we shouldn't try to run it
        if (!binary.exists()) {
            if (skipIfMissing) {
                getLog().info("Skipping missing test "+testName);
                return false;
            }else {
                throw new MojoExecutionException("Test "+binary+" was not built! (File does not exists.)");
            }
        }
        // If there is an explicit list of tests to run.it should include this test
        String testProp=session.getSystemProperties().getProperty("test");
        if (testProp != null) {
            String[] testPropArr = testProp.split(",");
            boolean found=false;
            for (String test : testPropArr) {
                if (test.equals(ALL_NATIVE)) {
                    found=true;
                    break;
                }
            }
            if (!found) {
                getLog().debug("did not find test '"+testName+"' in list "+testProp);
                return false;
            }
        }
        // All are the preconditions satisfied?
        if (preConditions != null) {
            int idx=1;
            for (Map.Entry<String, String> entry : preConditions.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key == null) {
                    throw new MojoExecutionException("NULL is not a valid precondition type. "+VALID_PRECONDITION_TYPES_STR);
                }
                if (key.equals("and")) {
                    if (!isTruthy(value)) {
                        getLog().info("Skipping test "+testName+" because precondition number "+idx+" was not met.");
                        return false;
                    }
                } else if (key.equals("andNot")) {
                    if (isTruthy(value)) {
                        getLog().info("Skipping test "+testName+" because negative precondition number "+idx+"was met.");
                        return false;
                    }
                }else {
                    throw new MojoExecutionException(key+" is not a valid precondition type. "+VALID_PRECONDITION_TYPES_STR);
                }
                idx++;
            }
        }
        return true;
    }

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (testName == null) {
            testName=binary.getName();
        }
        validatePlatform();
        validateParameters();
        if (!shouldRunTest()) {
            return;
        }
        if (!results.isDirectory()) {
            if (!results.mkdirs()) {
                throw new MojoExecutionException("Failed to create output directory '"+results+"'!");
            }
        }
        List<String> cmd=new LinkedList<>();
        cmd.add(binary.getAbsolutePath());
        getLog().info("--------------------------------------------");
        getLog().info(" C M A K E B U I L D E R   T E S T");
        getLog().info("--------------------------------------------");
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(testName).append(": running");
        stringBuilder.append(binary.getAbsolutePath());
        for (String arg : args) {
            cmd.add(arg);
            stringBuilder.append(" ").append(arg);
        }
        getLog().info(stringBuilder.toString());
        ProcessBuilder processBuilder = new ProcessBuilder(cmd);
        Exec.addEnvironment(processBuilder,env);
        if (workingDirectory != null) {
            processBuilder.directory(workingDirectory);
        }
        processBuilder.redirectError(new File(results,testName+".stderr"));
        processBuilder.redirectOutput(new File(results,testName+".stdout"));
        getLog().info("with extra environment variable "+Exec.envToString(env));
        Process process=null;
        TestThread testThread=null;
        int retCode=-1;
        String status="IN_PROGRESS";
        try {
            writeStatusFile(status);
        }catch (IOException e){
            throw new MojoExecutionException("Error writing the status file",e);
        }
        long start = System.nanoTime();
        try {
            process=processBuilder.start();
            testThread=new TestThread(process);
            testThread.start();
            testThread.join(timeout*1000);
            if (!testThread.isAlive()) {
                retCode=testThread.retCode;
                testThread=null;
                process=null;
            }
        }catch (IOException e){
            throw new MojoExecutionException("IOException while executing the test "+testName,e);
        }catch (InterruptedException e){
            throw new MojoExecutionException("Interrupted while executing the test "+testName,e);
        }finally {
            if (testThread != null) {
                // if the test thread didn't exist yet,that means the timeout expired
                testThread.interrupt();
                try {
                    testThread.join();
                }catch (InterruptedException e){
                    getLog().error("Interrupted while waiting for testThread",e);
                }
                status="TIMED OUT";
            } else if (retCode == 0) {
                status="SUCCESS";
            }else {
                status="ERROR CODE "+String.valueOf(retCode);
            }
            try {
                writeStatusFile(status);
            }catch (Exception e){
                getLog().error("failed to write status file!",e);
            }
            if (process != null) {
                process.destroy();
            }
        }
        long end = System.nanoTime();
        getLog().info("STATUS: "+status+" after "+ TimeUnit.MILLISECONDS.convert(end-start,TimeUnit.MILLISECONDS)+" millisecond(s).");
        getLog().info("-----------------------------------------------------");
        if (status.equals("TIMED OUT")) {
            if (expectedResult.equals("success")) {
                throw new MojoExecutionException("Test "+binary+" time out after "+timeout+" seconds!");
            }
        } else if (!status.equals("SUCCESS")) {
            if (expectedResult.equals("success")) {
                throw new MojoExecutionException("Test "+binary+" returned "+status);
            }
        } else if (expectedResult.equals("failure")) {
            throw new MojoExecutionException("Test "+binary+" succeeded, but we expected failure!");
        }
    }
}
