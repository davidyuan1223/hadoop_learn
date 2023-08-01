package org.apache.hadoop.security.token;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Token<T extends TokenIdentifier> implements Writable {
    public static final Logger LOG= LoggerFactory.getLogger(Token.class);
    private static Map<Text,Class<? extends TokenIdentifier>> tokenKindMap;
    private byte[] identifier;
    private byte[] password;
    private Text kind;
    private Text service;
    private TokenRenewer renewer;
    public Token(T id,SecretManager<T> mgr){
        password=mgr.createPassword(id);
        identifier=id.getBytes();
        kind=id.getKind();
        service=new Text();
    }
    public void setID(byte[] bytes){
        identifier=bytes;
    }
    public void setPassword(byte[] newPassword){
        password=newPassword;
    }
    public Token(byte[] identifier,byte[] password,Text kind,Text service){
        this.identifier=identifier==null?new byte[0]:identifier;
        this.password=password==null?new byte[0]:password;
        this.kind=kind==null?new Text():kind;
        this.service=service==null?new Text():service;
    }
    public Token(){
        identifier=new byte[0];
        password=new byte[0];
        kind=new Text();
        service=new Text();
    }
    public Token<T> copyToken(){
        return new Token<T>(this);
    }
    public byte[] getIdentifier(){
        return identifier;
    }
    private static Class<? extends TokenIdentifier> getClassForIdentifier(Text kind){
        Class<? extends TokenIdentifier> cls=null;
        synchronized (Token.class){
            if (tokenKindMap == null) {
                tokenKindMap= Maps.newHashMap();
                final Iterator<TokenIdentifier> identifierIterator= ServiceLoader.load(TokenIdentifier.class).iterator();
                while (identifierIterator.hasNext()) {
                    try {
                        TokenIdentifier id = identifierIterator.next();
                        tokenKindMap.put(id.getKind(),id.getClass());
                    }catch (ServiceConfigurationError | LinkageError e){
                        LOG.debug("Failed to load token identifier implementation",e);
                    }
                }
            }
            cls=tokenKindMap.get(kind);
        }
        if (cls == null) {
            LOG.debug("Can not find class for token kind {}",kind);
            return null;
        }
        return cls;
    }
    @SuppressWarnings("unchecked")
    public T decodeIdentifier()throws IOException{
        Class<? extends TokenIdentifier> cls=getClassForIdentifier(getKind());
        if (cls == null) {
            return null;
        }
        TokenIdentifier tokenIdentifier= ReflectionUtils.newInstance(cls,null);
        ByteArrayInputStream buf = new ByteArrayInputStream(identifier);
        DataInputStream in = new DataInputStream(buf);
        tokenIdentifier.readFields(in);
        in.close();
        return (T)tokenIdentifier;
    }
    public byte[] getPassword(){return password;}
    public synchronized Text getKind(){return kind;}
    @InterfaceAudience.Private
    public synchronized void setKind(Text newKind){
        kind=newKind;
        renewer=null;
    }

    public Text getService() {
        return service;
    }

    public void setService(Text service) {
        this.service = service;
    }
    public boolean isPrivate(){
        return false;
    }
    public boolean isPrivateCloneOf(Text thePublicService){
        return false;
    }

}
