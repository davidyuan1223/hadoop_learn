package cn.f33v.maven.plugin.protoc;

import cn.f33v.maven.plugin.util.Exec;
import cn.f33v.maven.plugin.util.FileSetUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.maven.model.FileSet;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.zip.CRC32;

/**
 * 主协议与测试协议的共同执行
 */
public class ProtocRunner {
    private final MavenProject project;
    private final File[] imports;
    private final File output;
    private final FileSet source;
    private final String protocCommand;
    private final String protocVersion;
    private final String checksumPath;
    private final boolean test;
    private final AbstractMojo mojo;

    @SuppressWarnings("checkstyle:parameternumber")
    public ProtocRunner(final MavenProject project,final File[] imports,final File output,
                        final FileSet source,final String protocCommand,final String protocVersion,
                        final String checksumPath,final AbstractMojo mojo,final boolean test){
        this.project = project;
        this.imports = Arrays.copyOf(imports, imports.length);
        this.output = output;
        this.source = source;
        this.protocCommand = protocCommand;
        this.protocVersion = protocVersion;
        this.checksumPath = checksumPath;
        this.mojo = mojo;
        this.test = test;
    }

    /**
     * 将包含文件和源文件校验和与存储在构建目录中的json文件中先前计算出的校验和进行比较
     */
    public class ChecksumComparator{
        private final Map<String ,Long> storedChecksums;
        private final Map<String ,Long> computedChecksums;
        private final File checksumFile;
        ChecksumComparator(String checksumPath) throws IOException {
            checksumFile=new File(checksumPath);
            if (checksumFile.exists()) {
                ObjectMapper mapper = new ObjectMapper();
                storedChecksums=mapper.readValue(checksumFile, new TypeReference<Map<String, Long>>() {
                });
            }else {
                storedChecksums=new HashMap<>(0);
            }
            computedChecksums=new HashMap<>();
        }
        public boolean hasChanged(File file) throws IOException {
            if (!file.exists()) {
                throw new FileNotFoundException("Specified protoc include or source not exists: "+file);
            }
            if (file.isDirectory()) {
                return hasDirectoryChanged(file);
            } else if (file.isFile()) {
                return hasFileChanged(file);
            }else {
                throw new IOException("Not a file or Directory: "+file);
            }
        }

        private boolean hasFileChanged(File file)throws IOException {
            long computedCsum = computeChecksum(file);
            Long storedCsum = storedChecksums.get(file.getCanonicalPath());
            if (storedCsum == null || storedCsum != computedCsum) {
                return true;
            }
            return false;
        }

        private long computeChecksum(File file) throws IOException{
            final String canonicalPath = file.getCanonicalPath();
            if (computedChecksums.containsKey(canonicalPath)) {
                return computedChecksums.get(canonicalPath);
            }
            CRC32 crc = new CRC32();
            byte[] buffer = new byte[1024 * 64];
            try (
                BufferedInputStream in=new BufferedInputStream(new FileInputStream(file))){
                while (true){
                    int read = in.read(buffer);
                    if (read <= 0) {
                        break;
                    }
                    crc.update(buffer,0,read);
                }
            }
            final long computedCsum=crc.getValue();
            computedChecksums.put(canonicalPath,computedCsum);
            return crc.getValue();
        }

        private boolean hasDirectoryChanged(File directory) throws IOException {
            File[] listFiles = directory.listFiles();
            boolean changed=false;
            if (listFiles == null) {
                return false;
            }
            for (File file : listFiles) {
                if (file.isDirectory()) {
                    if (hasDirectoryChanged(file)) {
                        changed=true;
                    }
                } else if (file.isFile()) {
                    if (hasFileChanged(file)) {
                        return true;
                    }
                }else {
                    mojo.getLog()
                            .debug("Skipping entry that is not a file or directory: "+file);
                }
            }
            return changed;
        }

        public void writeChecksums() throws IOException{
            ObjectMapper mapper = new ObjectMapper();
            Files.createDirectories(checksumFile.getParentFile().toPath());
            try (BufferedOutputStream out=new BufferedOutputStream(new FileOutputStream(checksumFile))){
                mapper.writeValue(out,computedChecksums);
                mojo.getLog().info("Wrote protoc checksums to file "+checksumFile);
            }
        }
    }
    public void execute() throws MojoExecutionException {
        try {
            List<String > command=new ArrayList<>();
            command.add(protocCommand);
            command.add("--version");
            Exec exec = new Exec(mojo);
            List<String > out=new ArrayList<>();
            if (exec.run(command,out)==127){
                mojo.getLog().error("protoc, not found at: "+protocCommand);
                throw new MojoExecutionException("protoc failure");
            }else {
                if (out.isEmpty()) {
                    mojo.getLog().error("stdout: "+out);
                    throw new MojoExecutionException("'protoc --version' did not return a version");
                }else {
                    if (!out.get(0).endsWith(protocVersion)) {
                        throw new MojoExecutionException(
                                "protoc version is "+out.get(0)+"', expected version is '" +protocVersion+"'"
                        );
                    }
                }
            }
            if (!output.mkdir()) {
                if (!output.exists()) {
                    throw new MojoExecutionException(
                            "Could not create directory: "+out
                    );
                }
            }

            ChecksumComparator comparator = new ChecksumComparator(checksumPath);
            boolean importsChanged=false;
            command=new ArrayList<>();
            command.add(protocCommand);
            command.add("--java_out="+output.getCanonicalPath());
            if (imports != null) {
                for (File anImport : imports) {
                    if (comparator.hasChanged(anImport)) {
                        importsChanged=true;
                    }
                    command.add("-I"+anImport.getCanonicalPath());
                }
            }
            List<File> changedSources=new ArrayList<>();
            boolean sourceChanged=false;
            for (File f : FileSetUtils.convertFileSetToFiles(source)) {
                if (comparator.hasChanged(f) || importsChanged) {
                    sourceChanged=true;
                    changedSources.add(f);
                    command.add(f.getCanonicalPath());
                }
            }
            if (!sourceChanged && !importsChanged) {
                mojo.getLog().info("No changes detected in protoc files, skipping generation");
            }else {
                if (mojo.getLog().isDebugEnabled()) {
                    StringBuilder b = new StringBuilder();
                    b.append("Generating classes for the following protoc files: [");
                    String prefix="";
                    for (File f : changedSources) {
                        b.append(prefix);
                        b.append(f.toString());
                        prefix=", ";
                    }
                    b.append("]");
                    mojo.getLog().debug(b.toString());
                }
                exec=new Exec(mojo);
                out=new ArrayList<>();
                List<String > err=new ArrayList<>();
                if (exec.run(command, out, err) != 0) {
                    mojo.getLog().error("protoc compiler error");
                    for (String s : out) {
                        mojo.getLog().error(s);
                    }
                    for (String s : err) {
                        mojo.getLog().error(s);
                    }
                    throw new MojoExecutionException("protoc failure");
                }
                comparator.writeChecksums();
            }
        }catch (Throwable e){
            throw new MojoExecutionException(e.toString(),e);
        }
        if (test) {
            project.addTestCompileSourceRoot(output.getAbsolutePath());
        }else {
            project.addCompileSourceRoot(output.getAbsolutePath());
        }
    }
}
