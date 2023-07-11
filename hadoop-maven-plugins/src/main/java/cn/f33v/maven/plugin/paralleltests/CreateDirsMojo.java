package cn.f33v.maven.plugin.paralleltests;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;

/**
 * 创建并行测试目录目标
 */
@Mojo(name = "parallel-tests-createdir",defaultPhase = LifecyclePhase.GENERATE_TEST_RESOURCES)
public class CreateDirsMojo extends AbstractMojo {
    /**
     * test.build.dir位置
     */
    @Parameter(defaultValue = "${project.build.directory}/test-dir")
    private File testBuildDir;
    /**
     * test.build.data位置
     */
    @Parameter(defaultValue = "${project.build.driectory}/test-dir")
    private File testBuildData;
    /**
     * test.build.data tmp位置
     */
    @Parameter(defaultValue = "${proect.build.directory}/tmp")
    private File hadoopTmpDir;
    /**
     * 线程数
     */
    @Parameter(defaultValue = "${testsThreadCount}")
    private String testsThreadCount;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        int numDirs=getTestsThreadCount();
        mkParallelDirs(testBuildDir,numDirs);
        mkParallelDirs(testBuildData,numDirs);
        mkParallelDirs(hadoopTmpDir,numDirs);
    }

    private void mkParallelDirs(File testDir, int numDirs) throws MojoExecutionException {
        for (int i = 0; i < numDirs; i++) {
            File newDir=new File(testDir,String.valueOf(i));
            if (!newDir.exists()) {
                getLog().info("Creating "+newDir.toString());
                if (!newDir.mkdirs()) {
                    throw new MojoExecutionException("Unable to create "+newDir.toString());
                }
            }
        }
    }

    public int getTestsThreadCount() {
        int threadCount=1;
        if (testsThreadCount != null) {
            String trimProp = testsThreadCount.trim();
            if (trimProp.endsWith("C")) {
                double multiplier=Double.parseDouble(
                        trimProp.substring(0,trimProp.length()-1)
                );
                double calculated=multiplier*((double)Runtime.getRuntime().availableProcessors() );
                threadCount=calculated>0d?Math.max((int) calculated,1):0;
            }else {
                threadCount=Integer.parseInt(testsThreadCount);
            }
        }
        return threadCount;
    }
}
