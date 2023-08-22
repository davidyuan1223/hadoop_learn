package org.apache.hadoop.fs.audit;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class CommonAuditContext {
    private static final Logger LOG= LoggerFactory.getLogger(CommonAuditContext.class);
    public static final String PROCESS_ID= UUID.randomUUID().toString();
    private static final Map<String ,String > GLOBAL_CONTEXT_MAP=new ConcurrentHashMap<>();
    private final Map<String , Supplier<String >> evaluatedEntries=new ConcurrentHashMap<>(1);
    static {
        setGlobalContextEntry(AuditConstant.PARAM_PROCESS,PROCESS_ID);
    }
    private static final ThreadLocal<CommonAuditContext> ACTIVE_CONTEXT=
            ThreadLocal.withInitial(CommonAuditContext::createInstance);
    private CommonAuditContext(){}
    public Supplier<String > put(String key,String value){
        if (value != null) {
            return evaluatedEntries.put(key,()->value);
        }else {
            return evaluatedEntries.remove(key);
        }
    }
    public Supplier<String > put(String key,Supplier<String > value){
        if (LOG.isTraceEnabled()) {
            LOG.trace("Adding context entry {}",key,new Exception(key));
        }
        return evaluatedEntries.put(key,value);
    }
    public void remove(String key){
        if (LOG.isTraceEnabled()) {
            LOG.trace("Remove context entry {}",key);
        }
        evaluatedEntries.remove(key);
    }
    public String get(String key){
        Supplier<String> supplier = evaluatedEntries.get(key);
        return supplier!=null?supplier.get():null;
    }
    public void reset(){
        evaluatedEntries.clear();
        init();
    }
    public void init(){
        put(AuditConstant.PARAM_THREAD0,CommonAuditContext::currentThreadID);
    }
    public boolean containsKey(String key){
        return evaluatedEntries.containsKey(key);
    }
    private static CommonAuditContext createInstance(){
        CommonAuditContext context = new CommonAuditContext();
        context.init();
        return context;
    }
    public static CommonAuditContext currentAuditContext(){
        return ACTIVE_CONTEXT.get();
    }
    public static String currentThreadID(){
        return Long.toString(Thread.currentThread().getId());
    }

    public Map<String, Supplier<String>> getEvaluatedEntries() {
        return evaluatedEntries;
    }
    public static void setGlobalContextEntry(String key,String value){
        GLOBAL_CONTEXT_MAP.put(key,value);
    }
    public static String getGlobalContextEntry(String key){
        return GLOBAL_CONTEXT_MAP.get(key);
    }
    public static void removeGlobalContextEntry(String key){
        GLOBAL_CONTEXT_MAP.remove(key);
    }
    public static void noteEntryPoint(Object tool){
        if (tool != null && !GLOBAL_CONTEXT_MAP.containsKey(AuditConstant.PARAM_COMMAND)) {
            String classname = tool.getClass().toString();
            int lastDot = classname.lastIndexOf('.');
            int l = classname.length();
            if (lastDot > 0 && lastDot < (l - 1)) {
                String name = classname.substring(lastDot + 1, l);
                setGlobalContextEntry(AuditConstant.PARAM_COMMAND,name);
            }
        }
    }
    public static Iterable<Map.Entry<String ,String >> getGlobalContextExtries(){
        return new GlobalIterable();
    }
    private static final class GlobalIterable implements Iterable<Map.Entry<String ,String >>{
        @Override
        public Iterator<Map.Entry<String, String>> iterator() {
            return GLOBAL_CONTEXT_MAP.entrySet().iterator();
        }
    }
}
