package cn.f33v.maven.plugin.util;

import org.apache.maven.plugin.Mojo;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 辅助程序类，用于从mojo执行外部进程
 */
public class Exec {
    private Mojo mojo;

    /**
     * 创建一个新的Exec实例用于从给定的mojo执行外部进程
     * @param mojo- Mojo执行外部进程
     */
    public Exec(Mojo mojo){
        this.mojo=mojo;
    }

    /**
     * 运行指定命令并将命令输出的每一行保存到给定列表中
     * @param command- 包含命令和所有参数的列表
     * @param output-列出输入/输出参数以接收命令输出
     * @return int，命令的退出代码
     */
    public int run(List<String > command,List<String > output){
        return this.run(command,output,null);
    }

    /**
     * 运行指定命令并将命令输出每一行保存到给定列表，并将命令stderr的每一行保存到另一个列表
     * @param command - 包含命令和所有参数列表
     * @param output - 列出输入/输出参数以接收命令输出
     * @param errors - 列出输入/输出参数以接收命令stderr
     * @return int 命令的退出代码
     */
    public int run(List<String > command,List<String > output,List<String > errors){
        int retCode=1;
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        try {
            Process process = processBuilder.start();
            OutputBufferedThread stdOut = new OutputBufferedThread(process.getInputStream());
            OutputBufferedThread stdErr = new OutputBufferedThread(process.getErrorStream());
            stdOut.start();
            stdErr.start();
            retCode= process.waitFor();
            if (retCode != 0) {
                mojo.getLog().warn(command+" failed with error code "+retCode);
                for (String s : stdErr.getOutput()) {
                    mojo.getLog().debug(s);
                }
            }
            stdOut.join();
            stdErr.join();
            output.addAll(stdOut.getOutput());
            if (errors != null) {
                errors.addAll(stdErr.getOutput());
            }
        } catch (IOException e) {
            mojo.getLog().warn(command + " failed: " + e.toString());
        } catch (InterruptedException e) {
            mojo.getLog().warn(command + " failed: " + e.toString());
        }
        return retCode;
    }

    /**
     * OutputBufferThread是一个后台线程，用于消耗和存储外部进程的输出
     */
    public static class OutputBufferedThread extends Thread{
        private List<String > output;
        private BufferedReader reader;

        /**
         * 创建一个新的OutputBufferThread来使用给定的InputStream
         * @param is - 要使用的输入流
         */
        public OutputBufferedThread(InputStream is){
            this.setDaemon(true);
            output=new ArrayList<>();
            try {
                reader=new BufferedReader(new InputStreamReader(is,"UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("Unsupported ending "+e.toString());
            }
        }

        @Override
        public void run() {
            try {
                String line = reader.readLine();
                while (line != null) {
                    output.add(line);
                    line=reader.readLine();
                }
            } catch (IOException e) {
                throw new RuntimeException("make failed with error code "+e.toString());
            }
        }

        public List<String> getOutput() {
            return output;
        }
    }

    /**
     * 将环境变量添加到ProcessBuilder
     * @param pb - 进程构建器
     * @param env - 环境变量名称到值的映射
     */
    public static void addEnvironment(ProcessBuilder pb, Map<String ,String > env){
        if (env == null) {
            return;
        }
        Map<String, String> processEnv = pb.environment();
        for (Map.Entry<String, String> entry : env.entrySet()) {
            String val = entry.getValue();
            if (val == null) {
                val="";
            }
            processEnv.put(entry.getKey(),val);
        }
    }

    /**
     * 打印环境变量
     * @param env - 环境映射map
     * @return - 打印字符串
     */
    public static String envToString(Map<String ,String > env){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{");
        if (env != null) {
            for (Map.Entry<String, String> entry : env.entrySet()) {
                String val = entry.getValue();
                if (val == null) {
                    val="";
                }
                stringBuilder.append("\n ")
                        .append(entry.getKey())
                        .append(" = '")
                        .append(val)
                        .append("'\n");
            }
        }
        stringBuilder.append("}");
        return stringBuilder.toString();
    }
}
