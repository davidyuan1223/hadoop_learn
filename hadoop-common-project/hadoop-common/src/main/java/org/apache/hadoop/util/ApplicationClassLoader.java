package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/22
 **/
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationClassLoader extends URLClassLoader {
    public static final String SYSTEM_CLASSES_DEFAULT;
    private static final String PROPERTIES_FILE="org.apache.hadoop.application-classloader.properties";
    private static final String SYSTEM_CLASSES_DEFAULT_KEY="system.classes.default";
    private static final Logger LOG= LoggerFactory.getLogger(ApplicationClassLoader.class.getName())
    static {
        try(InputStream is=ApplicationClassLoader.class.getClassLoader()
        .getResourceAsStream(PROPERTIES_FILE);){
            if (is == null) {
                throw new ExceptionInInitializerError("properties File "+PROPERTIES_FILE
                +" is not found");
            }
            Properties props = new Properties();
            props.load(is);
            String systemClassDefault = props.getProperty(SYSTEM_CLASSES_DEFAULT_KEY);
            if (systemClassDefault == null) {
                throw new ExceptionInInitializerError("property "+SYSTEM_CLASSES_DEFAULT_KEY+" is not found");
            }
            SYSTEM_CLASSES_DEFAULT=systemClassDefault;
        }catch (IOException e){
            throw new ExceptionInInitializerError(e);
        }
    }
    private final ClassLoader parent;
    private final List<String > systemClasses;
    public ApplicationClassLoader(URL[] urls, ClassLoader parent,List<String > systemClasses) {
        super(urls, parent);
        this.parent=parent;
        if (parent == null) {
            throw new IllegalArgumentException("No parent classLoader!");
        }
        this.systemClasses=(systemClasses==null || systemClasses.isEmpty())?
                Arrays.asList(StringUtils.getTrimmedString(SYSTEM_CLASSES_DEFAULT)):
                systemClasses;
        LOG.info("classpath: "+Arrays.toString(urls));
        LOG.info("system classes: "+this.systemClasses);
    }
    public ApplicationClassLoader(String classpath,ClassLoader parent,List<String > systemClasses) throws MalformedURLException {
        this(constructUrlsFromClasspath(classpath),parent,systemClasses);
    }
    static URL[] constructUrlsFromClasspath(String classpath)throws MalformedURLException {
        List<URL> urls=new ArrayList<>();
        for (String element : classpath.split(File.pathSeparator)) {
            if (element.endsWith(File.separator + "*")) {
                List<Path> jars= FileUtil.getJarInDirectory(element);
                if (!jars.isEmpty()) {
                    for (Path jar : jars) {
                        urls.add(jar.toUri().toURL());
                    }
                }
            }else {
                File file=new File(element);
                if (file.exists()) {
                    urls.add(new File(element).toURI().toURL());
                }
            }
        }
        return urls.toArray(new URL[urls.size()]);
    }

    @Override
    public URL getResource(String name) {
        URL url=null;
        if (!isSystemClass(name, systemClasses)) {
            url=findResource(name);
            if (url == null && name.startsWith("/")) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Remove leading / off "+name);
                }
                url=findResource(name.substring(1));
            }
        }
        if (url == null) {
            url=parent.getResource(name);
        }
        if (url != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getResource("+name+")="+url);
            }
        }
        return url;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return this.loadClass(name,false);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Loading class: "+name);
        }
        Class<?> c = findLoadedClass(name);
        ClassNotFoundException ex=null;
        if (c == null && !isSystemClass(name, systemClasses)) {
            try {
                c=findClass(name);
                if (LOG.isDebugEnabled() && c != null) {
                    LOG.debug("Loaded class: "+name+" ");
                }
            }catch (ClassNotFoundException e){
                if (LOG.isDebugEnabled()) {
                    LOG.debug(e.toString());
                }
                ex=e;
            }
        }
        if (c == null) {
            c=parent.loadClass(name);
            if (LOG.isDebugEnabled() && c != null) {
                LOG.debug("Loaded class from parent: "+name+" ");
            }
        }
        if (c == null) {
            throw ex!=null?ex:new ClassNotFoundException(name);
        }
        if (resolve) {
            resolveClass(c);
        }
        return c;
    }
    public static boolean isSystemClass(String name,List<String > systemClasses){
        boolean result=false;
        if (systemClasses != null) {
            String canonicalName = name.replace('/', '.');
            while (canonicalName.startsWith(".")) {
                canonicalName=canonicalName.substring(1);
            }
            for (String c : systemClasses) {
                boolean shouldInclude=true;
                if (c.startsWith("-")) {
                    c=c.substring(1);
                    shouldInclude=false;
                }
                if (canonicalName.startsWith(c)) {
                    if (c.endsWith(".")
                            || canonicalName.length() == c.length()
                            || canonicalName.length() > c.length()
                            && canonicalName.charAt(c.length()) == '$') {
                        if (shouldInclude) {
                            result=true;
                        }else {
                            return false;
                        }
                    }
                }
            }
        }
        return result;
    }
}
