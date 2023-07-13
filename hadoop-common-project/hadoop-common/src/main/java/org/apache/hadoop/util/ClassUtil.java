package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

@InterfaceAudience.Private
public class ClassUtil {
    /**
     * find a jar that contains a class of the same name,if any,it will return a jar file,
     * even if that is not the first thing on the class path that has a class with the same
     * name
     * @param clazz - the class to find
     * @return a jar file that contains the class or null
     */
    public static String findContainingJar(Class<?> clazz){
        ClassLoader loader = clazz.getClassLoader();
        String classFile = clazz.getName().replaceAll("\\.", "/") + ".class";
        try {
            for (final Enumeration<URL> itr=loader.getResources(classFile);
            itr.hasMoreElements();){
                final URL url=itr.nextElement();
                if ("jar".equals(url.getProtocol())) {
                    String toReturn=url.getPath();
                    if (toReturn.startsWith("file:")) {
                        toReturn=toReturn.substring("file:".length());
                    }
                    toReturn= URLDecoder.decode(toReturn,"UTF-8");
                    return toReturn.replaceAll("!.*$","");
                }
            }
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        return null;
    }
}
