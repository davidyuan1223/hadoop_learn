package cn.f33v.maven.plugin.versionInfo;

import cn.f33v.maven.plugin.util.Exec;
import org.apache.maven.model.FileSet;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * VersionInfoMojo计算有关代码库当前版本信息，并将该信息导出问属性，以便在maven构建中进一步使用
 * 版本信息包括构建时间，SCM URI，SCM分支，SCM提交以及代码库中文件内容的MD5校验和
 */
@Mojo(name = "version-info")
public class VersionInfoMojo extends AbstractMojo {
    @Parameter(defaultValue = "${project}",readonly = true)
    private MavenProject project;
    @Parameter(required = true)
    private FileSet source;
    @Parameter(defaultValue = "version-info.build.time")
    private String buildTimeProperty;
    @Parameter(defaultValue = "version-info.source.md5")
    private String md5Property;
    @Parameter(defaultValue = "version-info.scm.uri")
    private String scmUriProperty;
    @Parameter(defaultValue = "version-info.scm.branch")
    private String scmBranchProperty;
    @Parameter(defaultValue = "version-info.scm.commit")
    private String scmCommitProperty;
    @Parameter(defaultValue = "git")
    private String gitCommand;
    private enum SCM {NONE,GIT}
    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            SCM scm = determineSCM();
            project.getProperties().setProperty(buildTimeProperty,getBuildTime());
            project.getProperties().setProperty(scmUriProperty,getSCMUri(scm));
            project.getProperties().setProperty(scmBranchProperty,getSCMBranch(scm));
            project.getProperties().setProperty(scmCommitProperty,getSCMCommit(scm));
        }
    }

    private String getSCMCommit(SCM scm) {
        String commit="Unknown";
        if (scm == SCM.GIT) {
            for (String out : scmOut) {
                if (out.startsWith("commit")) {
                    commit=out.substring("commit".length());
                    break;
                }
            }
        }
        return commit.trim();
    }

    private String getSCMBranch(SCM scm) {
        String branch="Unknown";
        if (scm == SCM.GIT) {
            for (String out : scmOut) {
                if (out.startsWith("*")) {
                    branch=out.substring("*".length());
                    break;
                }
            }
        }
        return branch.trim();
    }

    private String getSCMUri(SCM scm) {
        String uri="Unknown";
        if (scm == SCM.GIT) {
            for (String out : scmOut) {
                if (out.startsWith("origin") && out.endsWith("(fetch)")) {
                    uri = out.substring("origin".length());
                    uri = uri.substring(0, uri.length() - "(fetch)".length());
                    break;
                }
            }
        }
        return uri.trim();
    }

    /**
     * 获取构建时间
     * @return 构建时间
     */
    private String getBuildTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.format(new Date());
    }

    private List<String > scmOut;
    /**
     * 确定正在使用哪个SCM（git或无）并捕获SCM命令的输出以供以后解析
     * @return 用于此构建的SCM
     */
    private SCM determineSCM() {
        Exec exec = new Exec(this);
        SCM scm = SCM.NONE;
        scmOut=new ArrayList<>();
        int ret;
        ret=exec.run(Arrays.asList(gitCommand,"branch"),scmOut);
        if (ret==0) {
            ret=exec.run(Arrays.asList(gitCommand,"remote","-v"),scmOut);
            if (ret != 0) {
                scm=SCM.NONE;
                scmOut=null;
            }else {
                ret=exec.run(Arrays.asList(gitCommand,"log","-n","1"),scmOut);
                if (ret!=0) {
                    scm=SCM.NONE;
                    scmOut=null;
                }else {
                    scm=SCM.GIT;
                }
            }
        }
        if (scmOut!=null) {
            getLog().debug(scmOut.toString());
        }
        getLog().info("SCM: "+scm);
        return scm;
    }
}
