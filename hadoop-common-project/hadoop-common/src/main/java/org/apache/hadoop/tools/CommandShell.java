package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.PrintStream;

public abstract class CommandShell extends Configured implements Tool {
    private PrintStream out=System.out;
    private PrintStream err=System.err;
    private SubCommand subCommand=null;
    public abstract String getCommandUsage();
    public void setSubCommand(SubCommand cmd){
        subCommand=cmd;
    }
    public void setOut(PrintStream p){
        out=p;
    }

    public PrintStream getOut() {
        return out;
    }

    public void setErr(PrintStream err) {
        this.err = err;
    }

    public PrintStream getErr() {
        return err;
    }

    @Override
    public int run(String[] args) throws Exception {
        int exitCode=0;
        try {
            exitCode=init(args);
            if (exitCode != 0 || subCommand == null) {
                printShellUsage();
                return exitCode;
            }
            if (subCommand.validate()) {
                subCommand.execute();
            }else {
                printShellUsage();
                exitCode=1;
            }
        }catch (Exception e){
            printShellUsage();
            printException(e);
            return 1;
        }
        return exitCode;
    }
    protected abstract int init(String [] args)throws Exception;
    protected final void printShellUsage(){
        if (subCommand != null) {
            out.println(subCommand.getUsage());
        }else {
            out.println(getCommandUsage());
        }
        out.flush();
    }
    protected void printException(Exception e){
        e.printStackTrace(err);
    }

    protected abstract class SubCommand{
        public boolean validate(){return true;}
        public abstract void execute()throws Exception;
        public abstract String getUsage();
    }
}
