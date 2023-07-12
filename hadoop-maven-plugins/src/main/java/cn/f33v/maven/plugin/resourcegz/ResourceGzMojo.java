package cn.f33v.maven.plugin.resourcegz;

import org.apache.commons.io.IOUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.zip.GZIPOutputStream;

/**
 * ResourceGzMojo will gzip files,it is meant to be used for gzipping website files(e.g.js,.css,etc). It
 * takes an input directory,output directory,and extensions to process and will generate the .gz fils. Any additional
 * directory structure beyond the input directory is preserved in the output directory
 */
@Mojo(name = "resource-gz")
public class ResourceGzMojo extends AbstractMojo {
    /**
     * The input directory,will be searched recursively and its directory structure will be maintained
     * in the outputDirectory
     */
    @Parameter(property = "inputDirectory",required  = true)
    private String inputDirectory;
    /**
     * the output directory
     */
    @Parameter(property = "outputDirectory",required = true)
    private String outputDirectory;
    /**
     * a comma separated list of extensions to include
     */
    @Parameter(property = "extensions",required = true)
    private String extensions;

    private class GZConsumer implements Consumer<Path>{
        private final File inputDir;
        private final File outputDir;
        private Throwable throwable;

        public GZConsumer(File inputDir, File outputDir) {
            this.inputDir = inputDir;
            this.outputDir = outputDir;
            this.throwable=null;
        }

        @Override
        public void accept(Path path) {
            if (throwable != null) {
                return;
            }
            try {
                File outputFile = new File(outputDir, path.toFile().getCanonicalPath()
                        .replaceFirst(Matcher.quoteReplacement(inputDir.getCanonicalPath()), "") + ".gz");
                if (outputFile.getParentFile().isDirectory()
                        || outputFile.getParentFile().mkdirs()) {
                    try (
                        GZIPOutputStream os = new GZIPOutputStream(new FileOutputStream(outputFile));
                        BufferedReader is = Files.newBufferedReader(path);
                    ){
                        getLog().info("Compressing "+path+" to "+outputFile);
                        IOUtils.copy(is,os, StandardCharsets.UTF_8);
                    }
                }else {
                    throw new IOException("Directory " + outputFile.getParent()
                            + " does not exist or was unable to be created");
                }
            }catch (Throwable t) {
                this.throwable = t;
            }
        }

        public Throwable getThrowable() {
            return throwable;
        }
    }

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            Path inputPath = new File(inputDirectory).toPath();
            File outputDir = new File(outputDirectory);
            List<String > exts= Arrays.asList(extensions.split(","));
            exts.replaceAll(String::trim);
            GZConsumer cons = new GZConsumer(inputPath.toFile(), outputDir);
            Files.walk(inputPath).filter(path->{
                for (String ext : exts) {
                    if (path.getFileName().toString().endsWith("."+ext)){
                        return true;
                    }
                }
                return false;
            }).forEach(cons);
            if (cons.getThrowable() != null) {
                throw new MojoExecutionException(cons.getThrowable().toString(),cons.getThrowable());
            }
        }catch (Throwable t){
            throw new MojoExecutionException(t.toString(),t);
        }
    }
}
