package com.apache.hadoop.classification.tools;


import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.sun.javadoc.*;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Process the RootDoc by substituting with (nested) proxy objects that exclude elements with
 * Private or LimitedPrivate annotations
 */
class RootDocProcessor {
    static String stability = StabilityOptions.UNSTABLE_OPTION;
    static boolean treatUnannotatedClassesAsPrivate = false;
    public static RootDoc process(RootDoc rootDoc){return (RootDoc) process(rootDoc,RootDoc.class);}
    private static Object process(Object obj,Class<?> type){
        if (obj == null) {
            return null;
        }
        Class<?> objClass = obj.getClass();
        if (objClass.getName().startsWith("com.sun.")) {
            return getProxy(obj);
        }else if (obj instanceof Object[]){
            Class<?> componentType = type.isArray() ? type.getComponentType() : objClass.getComponentType();
            Object[] array= (Object[]) obj;
            Object[] newArray=(Object[]) Array.newInstance(componentType,array.length);
            for (int i = 0; i < array.length; i++) {
                newArray[i]=process(array[i],componentType);
            }
            return newArray;
        }
        return obj;
    }

    private static Map<Object,Object> proxies=new WeakHashMap<>();
    private static Object getProxy(Object obj) {
        Object proxy = proxies.get(obj);
        if (proxy == null) {
            proxy= Proxy.newProxyInstance(obj.getClass().getClassLoader(),
                    obj.getClass().getInterfaces(),new ExcludeHandler(obj));
            proxies.put(obj,proxy);
        }
        return proxy;
    }

    private static class ExcludeHandler implements InvocationHandler {
        private Object target;

        public ExcludeHandler(Object target) {
            this.target=target;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();
            if (target instanceof Doc) {
                if (methodName.equals("isIncluded")) {
                    Doc doc = (Doc) target;
                    return !exclude(doc) && doc.isIncluded();
                }
                if (target instanceof RootDoc) {
                    switch (methodName) {
                        case "classes":
                            return filter(((RootDoc) target).classes(), ClassDoc.class);
                        case "specifiedClasses":
                            return filter(((RootDoc) target).specifiedClasses(), ClassDoc.class);
                        case "specifiedPackages":
                            return filter(((RootDoc) target).specifiedPackages(), ClassDoc.class);
                    }
                } else if (target instanceof ClassDoc) {
                    switch (methodName) {
                        case "methods":
                            return filter(((ClassDoc) target).methods(true), MethodDoc.class);
                        case "fields":
                            return filter(((ClassDoc) target).fields(true), FieldDoc.class);
                        case "innerClasses":
                            return filter(((ClassDoc) target).innerClasses(true), ClassDoc.class);
                        case "constructors":
                            return filter(((ClassDoc) target).constructors(true), ConstructorDoc.class);
                    }
                } else {
                    if (methodName.equals("methods")) {
                        return filter(((ClassDoc) target).methods(true), MethodDoc.class);
                    }
                }
            } else if (target instanceof PackageDoc) {
                if (methodName.equals("allClasses")) {
                    if (isFiltered(args)) {
                        return filter(((PackageDoc) target).allClasses(true),
                                ClassDoc.class);
                    } else {
                        return filter(((PackageDoc) target).allClasses(), ClassDoc.class);
                    }
                } else if (methodName.equals("annotationTypes")) {
                    return filter(((PackageDoc) target).annotationTypes(),
                            AnnotationTypeDoc.class);
                } else if (methodName.equals("enums")) {
                    return filter(((PackageDoc) target).enums(),
                            ClassDoc.class);
                } else if (methodName.equals("errors")) {
                    return filter(((PackageDoc) target).errors(),
                            ClassDoc.class);
                } else if (methodName.equals("exceptions")) {
                    return filter(((PackageDoc) target).exceptions(),
                            ClassDoc.class);
                } else if (methodName.equals("interfaces")) {
                    return filter(((PackageDoc) target).interfaces(),
                            ClassDoc.class);
                } else if (methodName.equals("ordinaryClasses")) {
                    return filter(((PackageDoc) target).ordinaryClasses(),
                            ClassDoc.class);
                }
            }
            if (args != null) {
                if (methodName.equals("compareTo") || methodName.equals("equals")
                        || methodName.equals("overrides")
                        || methodName.equals("subclassOf")) {
                    args[0] = unwrap(args[0]);
                }
            }
            try {
                return process(method.invoke(target, args), method.getReturnType());
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }

        private static boolean exclude(Doc doc){
            AnnotationDesc[] annotations=null;
            if (doc instanceof ProgramElementDoc){
                annotations=((ProgramElementDoc)doc).annotations();
            } else if (doc instanceof PackageDoc) {
                annotations=((PackageDoc)doc).annotations();
            }
            if (annotations != null) {
                for (AnnotationDesc annotation : annotations) {
                    String qualifiedTypeName = annotation.annotationType().qualifiedTypeName();
                    if (qualifiedTypeName.equals(InterfaceAudience.Private.class.getCanonicalName())
                            ||qualifiedTypeName.equals(InterfaceAudience.LimitedPrivate.class.getCanonicalName())){
                        return true;
                    }
                    if (stability.equals(StabilityOptions.EVOLVING_OPTION)) {
                        if (qualifiedTypeName.equals(InterfaceStability.Unstable.class.getCanonicalName())) {
                            return true;
                        }
                    }
                    if (stability.equals(StabilityOptions.STABLE_OPTION)) {
                        if (qualifiedTypeName.equals(InterfaceStability.Evolving.class.getCanonicalName())
                                || qualifiedTypeName.equals(InterfaceStability.Unstable.class.getCanonicalName())) {
                            return true;
                        }
                    }
                }
                for (AnnotationDesc annotation : annotations) {
                    String qualifiedTypeName = annotation.annotationType().qualifiedTypeName();
                    if (qualifiedTypeName.equals(InterfaceAudience.Public.class.getCanonicalName())) {
                        return false;
                    }
                }
            }
            if (treatUnannotatedClassesAsPrivate) {
                return doc.isClass() || doc.isInterface() || doc.isAnnotationType();
            }
            return false;
        }
        private static Object[] filter(Doc[] array,Class<?> componentType){
            if (array == null || array.length == 0) {
                return array;
            }
            List<Object> list=new ArrayList<>(array.length);
            for (Doc doc : array) {
                if (!exclude(doc)) {
                    list.add(process(doc,componentType));
                }
            }
            return list.toArray((Object[])Array.newInstance(componentType,list.size()));
        }
        private Object unwrap(Object proxy){
            if (proxy instanceof Proxy) {
                return ((ExcludeHandler)Proxy.getInvocationHandler(proxy)).target;
            }
            return proxy;
        }

        private boolean isFiltered(Object[] args){
            return args!=null && Boolean.TRUE.equals(args[0]);
        }
    }
}
