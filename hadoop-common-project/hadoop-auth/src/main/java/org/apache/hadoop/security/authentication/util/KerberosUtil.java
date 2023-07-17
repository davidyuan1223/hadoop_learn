package org.apache.hadoop.security.authentication.util;

import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.kerberos.KeyTab;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

import static org.apache.hadoop.util.PlatformName.IBM_JAVA;
/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/16
 **/
public class KerberosUtil {
    public static String getKrb5LoginModuleName(){
        return (IBM_JAVA)
                ? "com.ibm.security.auth.module.Krb5LoginModule"
                : "com.sun.security.auth.module.Krb5LoginModule";
    }
    public static final Oid GSS_SPNEGO_MECH_OID=
            getNumericOidInstance("1.3.6.1.5.5.2");
    public static final Oid GSS_KRB5_MECH_OID=
            getNumericOidInstance("1.2.840.113554.1.2.2");
    public static final Oid NT_GSS_KRB5_PRINCIPAL_OID=
            getNumericOidInstance("1.2.840.113554.1.2.2.1");

    @Deprecated
    public static Oid getOidInstance(String oidName) throws NoSuchFieldException {
        switch (oidName){
            case "GSS_SPNEGO_MECH_OID":
                return GSS_SPNEGO_MECH_OID;
            case "GSS_KRB5_MECH_OID":
                return GSS_KRB5_MECH_OID;
            case "NT_GSS_KRB5_PRINCIPAL":
                return NT_GSS_KRB5_PRINCIPAL_OID;
            default:
                throw new NoSuchFieldException(
                        "oidName: "+oidName+" is not supported."
                );
        }
    }

    private static Oid getNumericOidInstance(String oidName) {
        try {
            return new Oid(oidName);
        }catch (GSSException e){
            throw new IllegalArgumentException(e);
        }
    }

    public static String getDefaultRealm(){
        return new KerberosPrincipal("tmp",1).getRealm();
    }

    public static String getDefaultRealmProtected(){
        try {
            return getDefaultRealm();
        }catch (Exception e){
            return null;
        }
    }

    public static String getDomainRealm(String shortprinc){
        Class<?> classRef;
        Object principalName;
        String realmString=null;
        try {
            if (IBM_JAVA) {
                classRef=Class.forName("com.ibm.security.krb5.PrincipalName");
            }else {
                classRef=Class.forName("sun.security.krb5.PrincipalName");
            }
            int tKrbNtSrvHst=classRef.getField("KRB_NT_SRV_HST").getInt(null);
            principalName=classRef.getConstructor(String.class,int.class)
                    .newInstance(shortprinc,tKrbNtSrvHst);
            realmString=(String)classRef.getMethod("getRealmString",new Class[0])
                    .invoke(principalName,new Object[0]);
        }catch (RuntimeException e){

        }catch (Exception e){

        }
        if (null == realmString || realmString.equals("")) {
            return getDefaultRealmProtected();
        }else {
            return realmString;
        }
    }
    public static String getLocalHostName()throws UnknownHostException{
        return InetAddress.getLocalHost().getCanonicalHostName();
    }
    public static final String getServicePrincipal(String service,String hostname) throws UnknownHostException {
        String fqdn=hostname;
        String shortprinc=null;
        String realmString=null;
        if (null == fqdn || fqdn.equals("") || fqdn.equals("0.0.0.0")) {
            fqdn=getLocalHostName();
        }
        fqdn=fqdn.toLowerCase(Locale.US);
        shortprinc=service+"/"+fqdn;
        realmString=getDomainRealm(shortprinc);
        if (null == realmString || realmString.equals("")) {
            return shortprinc;
        }else {
            return shortprinc+"@"+realmString;
        }
    }
    static final String[] getPrincipalNames(String keytabFileName) throws IOException {
        Keytab keytab=Keytab.loadKeytab(new File(keytabFileName));
        Set<String > principals=new HashSet<>();
        List<PrincipalName> entries = keytab.getPrincipals();
        for (PrincipalName entry : entries) {
            principals.add(entry.getName().replace("\\","/"));
        }
        return principals.toArray(new String[0]);
    }
    public static final String[] getPrincipalNames(String keytab, Pattern pattern) throws IOException {
        String[] principals = getPrincipalNames(keytab);
        if (principals.length != 0) {
            List<String > matchingPrincipals=new ArrayList<>();
            for (String principal : principals) {
                if (pattern.matcher(principal).matches()) {
                    matchingPrincipals.add(principal);
                }
            }
            principals=matchingPrincipals.toArray(new String[0]);
        }
        return principals;
    }
    public static boolean hasKerberosKeyTab(Subject subject){
        return !subject.getPrivateCredentials(KeyTab.class).isEmpty();
    }
    public static boolean hasKerberosTicket(Subject subject){
        return !subject.getPrivateCredentials(KerberosTicket.class).isEmpty();
    }
    public static String getTokenServerName(byte[] rawToken){
        DER token = new DER(rawToken);
        DER oid = token.next();
        if (oid.equals(DER.SPNEGO_MECH_OID)) {
            token=token.next().get(0xa0,0x30,0xa2,0x04).next();
            oid=token.next();
        }
        if (!oid.equals(DER.KRB5_MECH_OID)) {
            throw new IllegalArgumentException("Malformed gss token");
        }
        if (token.next().getTag() != 1) {
            throw new IllegalArgumentException("Not an AP-REQ token");
        }
        DER ticket = token.next().get(0x6e, 0x30, 0xa3, 0x61, 0x30);
        String realm = ticket.get(0xa1, 0x1b).getAsString();
        DER names = ticket.get(0xa2, 0x30, 0xa1, 0x30);
        StringBuilder sb = new StringBuilder();
        while (names.hasNext()) {
            if (sb.length() > 0) {
                sb.append('/');
            }
            sb.append(names.next().getAsString());
        }
        return sb.append('@').append(realm).toString();
    }


    private static class DER implements Iterator<DER>{
        static final DER SPNEGO_MECH_OID=getDER(GSS_SPNEGO_MECH_OID);
        static final DER KRB5_MECH_OID=getDER(GSS_KRB5_MECH_OID);
        private final int tag;
        private final ByteBuffer bb;

        DER(byte[] buf){this(ByteBuffer.wrap(buf));}
        DER(ByteBuffer srcbb){
            tag=srcbb.get() & 0xff;
            int length=readLength(srcbb);
            bb=srcbb.slice();
            bb.limit(length);
            srcbb.position(srcbb.position()+length);
        }

        private static int readLength(ByteBuffer srcbb) {
            int length=srcbb.get();
            if ((length & (byte) 0x80) != 0) {
                int varlength=length&0x7f;
                length=0;
                for (int i = 0; i < varlength; i++) {
                    length=(length<<8)|(srcbb.get()&0xff);
                }
            }
            return length;
        }

        private static DER getDER(Oid oid) {
            try {
                return new DER(oid.getDER());
            }catch (GSSException e){
                throw new IllegalArgumentException(e);
            }
        }

        DER choose(int subtag){
            while (hasNext()){
                DER der=next();
                if (der.getTag()==subtag){
                    return der;
                }
            }
            return null;
        }

        DER get(int... tags){
            DER der=this;
            for (int i = 0; i < tags.length; i++) {
                int expectedTag=tags[i];
                if (der.getTag()!=expectedTag){
                    der=der.hasNext()?der.choose(expectedTag):null;
                }
                if (der == null) {
                    StringBuilder sb = new StringBuilder("Tag not found");
                    for (int j = 0; j < i; j++) {
                        sb.append("0x").append(Integer.toHexString(tags[j]));
                    }
                    throw new IllegalArgumentException(sb.toString());
                }
            }
            return der;
        }

        String getAsString(){
            return new String(bb.array(),bb.arrayOffset()+bb.position(),
                    bb.remaining(), StandardCharsets.UTF_8);
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof DER)&&tag==((DER)o).tag&&bb.equals(((DER)o).bb);
        }



        @Override
        public int hashCode() {
            return 31*tag+bb.hashCode();
        }


        @Override
        public boolean hasNext() {
            return ((tag&0x30)!=0 || tag==0x04)&&bb.hasRemaining();
        }

        @Override
        public DER next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return new DER(bb);
        }

        @Override
        public String toString() {
            return "[tag=0x"+Integer.toHexString(tag)+" bb="+bb+"]";
        }

        int getTag(){return tag;}
    }
}
