package cn.f33v.maven.plugin.protoc;

import org.apache.maven.model.FileSet;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;

/**
 * Mojo使用protoc从.proto文件生成java类
 */
@Mojo(name = "protoc",defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class ProtoMojo  extends AbstractMojo {
    @Parameter(defaultValue = "${project}",readonly = true)
    private MavenProject project;
    @Parameter
    private File[] imports;
    @Parameter(defaultValue = "${project.build.directory}/generated-sources/java")
    private File output;
    @Parameter(readonly = true)
    private FileSet source;
    @Parameter(defaultValue = "protoc")
    private String protocCommand;
    @Parameter(readonly = true)
    private String protocVersion;
    @Parameter(defaultValue = "${project.build.directory}/hadoop-maven-plugins-protoc-checksums.json")
    private String checksumPath;
    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        final ProtocRun
    }
}
