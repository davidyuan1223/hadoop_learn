package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce","YARN"})
@InterfaceStability.Unstable
public final class ExitUtil {
    private static final Logger logger= LoggerFactory.getLogger(ExitUtil.class.getName());
    private static volatile boolean systemExitDisabled = false;
    private static volatile boolean systemHaltDisabled=false;
    private static final AtomicReference<ExitException> FIRST_EXIT_EXCEPTION=new AtomicReference<>();
    private static final AtomicReference<HaltException> FIRST_HALT_EXCEPTION=new AtomicReference<>();
    public static final String EXIT_EXCEPTION_MESSAGE="ExitException";
    public static final String HALT_EXCEPTION_MESSAGE="HaltException";
    private ExitUtil(){}
    public static void disableSystemExit(){systemExitDisabled=true;}
    public static void disableSystemHalt(){systemHaltDisabled=true;}
    public static boolean terminateCalled(){
        return FIRST_EXIT_EXCEPTION.get()!=null;
    }
    public static boolean haltCalled(){
        return FIRST_HALT_EXCEPTION.get()!=null;
    }
    public static ExitException getFirstExitException(){
        return FIRST_EXIT_EXCEPTION.get();
    }
    public static HaltException getFirstHaltException(){
        return FIRST_HALT_EXCEPTION.get();
    }
    public static void resetFirstExitException(){FIRST_EXIT_EXCEPTION.set(null);}
    public static void resetFirstHaltException(){FIRST_HALT_EXCEPTION.set(null);}
    private static <T extends Throwable> T addSuppressed(T suppressor,T suppressed){
        if (suppressor == null) {
            return suppressed;
        }
        if (suppressor!=suppressed){
            suppressor.addSuppressed(suppressed);
        }
        return suppressor;
    }
    public static void terminate(final ExitException ee)throws ExitException{
        final int status=ee.getExitCode();
        Error caught=null;
        if (status!=0) {
            try {
                String message = ee.getMessage();
                logger.debug("Exiting with status {}: {}",status,message,ee);
                logger.info("Exiting with status {}: {}",status,message);
            }catch (Error e){
                caught=e;
            }catch (Throwable t){
                addSuppressed(ee,t);
            }
        }
        if (systemExitDisabled) {
            try {
                logger.error("Terminate called",ee);
            } catch (Throwable e) {
                addSuppressed(ee,e);
            }
        }
    }
    public static class ExitException extends RuntimeException implements ExitCodeProvider{
        private static final long serialVersionUID=1L;
        private final int status;
        public ExitException(int status,String msg){
            super(msg);
            this.status=status;
        }
        public ExitException(int status,String message,Throwable cause){
            super(message,cause);
            this.status=status;
        }
        public ExitException(int status,Throwable cause){
            super(cause);
            this.status=status;
        }
        @Override
        public int getExitCode() {
            return status;
        }

        @Override
        public String toString() {
            String message = getMessage();
            if (message == null) {
                message=super.toString();
            }
            return status +": "+message;
        }
    }

    /**
     * An exception raised when a call to terminate was called and system halts were blocked
     */
    public static class HaltException extends RuntimeException implements ExitCodeProvider{
        private static final long serialVersionUID=1L;
        private final int status;
        public HaltException(int status,String msg){
            super(msg);
            this.status=status;
        }
        public HaltException(int status,String message,Throwable cause){
            super(message,cause);
            this.status=status;
        }
        public HaltException(int status,Throwable cause){
            super(cause);
            this.status=status;
        }
        @Override
        public int getExitCode() {
            return status;
        }

        @Override
        public String toString() {
            String message = getMessage();
            if (message == null) {
                message=super.toString();
            }
            return status +": "+message;
        }
    }
}
