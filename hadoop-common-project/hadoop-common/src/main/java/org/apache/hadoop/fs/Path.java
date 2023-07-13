package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.avro.reflect.Stringable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputValidation;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

@Stringable
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Path implements Comparable<Path>, Serializable, ObjectInputValidation {
    public static final String SEPARATOR="/";
    public static final char SEPARATOR_CHAR='/';
    public static final String CUD_DIR=".";
    public static final boolean WINDOWS=System.getProperty("os.name").startsWith("Windows");
    private static final Pattern HAS_DRIVE_LETTER_SPECIFIER = Pattern.compile("^/?[a-zA-Z]:");
    private static final Pattern SLASHES=Pattern.compile("/+");
    private static final long serialVersionUID=0xad00f;
    private URI uri;
    public Path(String parent,String child){this(new Path(parent),new Path(child));}
    public Path(Path parent,String  child){this(parent,new Path(child));}
    public Path(URI aUri){uri=aUri.normalize();}
    public Path(String parent,Path child){this(new Path(parent),child);}
    public Path(Path parent,Path child){
        URI parentUri = parent.uri;
        String parentPath = parentUri.getPath();
        if (!(parentPath.equals("/") || parentPath.isEmpty())) {
            try {
                parentUri=new URI(parentUri.getScheme(),parentUri.getAuthority(),
                        parentUri.getPath()+"/",null,parentUri.getFragment());
            }catch (URISyntaxException e){
                throw new IllegalArgumentException(e);
            }
        }
        URI resolved = parentUri.resolve(child.uri);
        initialize(resolved.getScheme(),resolved.getAuthority(),
                resolved.getPath(),resolved.getFragment());
    }
    public Path(String pathString)throws IllegalAccessError{
        checkPathArg(pathString);
        if (hasWindowsDrive(pathString) && pathString.charAt(0) != '/') {
            pathString="/"+pathString;
        }
        String scheme=null;
        String authority=null;
        int start=0;
        int colon=pathString.indexOf(":");
        int slash=pathString.indexOf("/");
        if ((colon != -1) && ((slash == -1) || (colon < slash))) {
            scheme=pathString.substring(0,colon);
            start=colon+1;
        }
        if (pathString.startsWith("//", start) && (pathString.length() - start > 2)) {
            int nextSlash = pathString.indexOf('/', start + 2);
            int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority=pathString.substring(start+2,authEnd);
            start=authEnd;
        }
        String path = pathString.substring(start, pathString.length());
        initialize(scheme,authority,path,null);
    }

    public Path(String scheme,String authority,String path){
        checkPathArg(path);
        if (hasWindowsDrive(path) && path.charAt(0) != '/') {
            path="/"+path;
        }
        if (!WINDOWS && path.charAt(0) != '/') {
            path="./"+path;
        }
        initialize(scheme,authority,path,null);
    }
    private void initialize(String scheme,String authority,String path,String fragment){
        try {
            this.uri=new URI(scheme,authority,normalizePath(scheme,path),null,fragment).normalize();
        }catch (URISyntaxException e){
            throw new IllegalArgumentException(e);
        }
    }

    private static String normalizePath(String scheme, String path) {
        path=SLASHES.matcher(path).replaceAll("/");
        if (WINDOWS&&(
                hasWindowsDrive(path)||(scheme==null)
                ||(scheme.isEmpty())||(scheme.equals("file"))
                )){
            path= StringUtils.replace(path,"\\","/");
        }
        int minLength=startPositionWithoutWindowsDrive(path)+1;
        if (path.length() > minLength && path.endsWith(SEPARATOR)) {
            path=path.substring(0,path.length()-1);
        }
        return path;
    }

    private void checkPathArg(String pathString) {
        if (pathString == null) {
            throw new IllegalArgumentException("Can not create a Path from a null string");
        }
        if (pathString.length() == 0) {
            throw new IllegalArgumentException("Can not create a Path from an empty string");
        }
    }

    void checkNotSchemeWithRelative(){
        if (toUri().isAbsolute()&&!isUriPathAbsolute()){
            throw new HadoopIllegalArgumentException(
                    "Unsupported name: has scheme but relative path-part"
            );
        }
    }

    void checkNotRelative(){
        if (!isAbsolute() && toUri().getScheme() == null) {
            throw new HadoopIllegalArgumentException("Path is relative");
        }
    }

    public static Path getPathWithoutSchemeAndAuthority(Path path){
        Path newPath = path.isUriPathAbsolute() ? new Path(null, null, path.toUri().getPath()) : path;
        return newPath;
    }

    public boolean isAbsolute() {
        return isUriPathAbsolute();
    }

    private boolean isUriPathAbsolute() {
        int start=startPositionWithoutWindowsDrive(uri.getPath());
        return uri.getPath().startsWith(SEPARATOR,start);
    }

    private static int startPositionWithoutWindowsDrive(String path) {
        if (hasWindowsDrive(path)) {
            return path.charAt(0)==SEPARATOR_CHAR?3:2;
        }else {
            return 0;
        }
    }

    private static boolean hasWindowsDrive(String path) {
        return (WINDOWS&&HAS_DRIVE_LETTER_SPECIFIER.matcher(path).find());
    }

    private URI toUri() {
        return uri;
    }

    public static Path mergePaths(Path path1,Path path2){
        String path2Str = path2.toUri().getPath();
        path2Str=path2Str.substring(startPositionWithoutWindowsDrive(path2Str));
        return new Path(path1.toUri().getScheme(),path1.toUri().getAuthority(),path1.toUri().getPath()+path2Str);
    }

    public static boolean isWindowsAbsolutePath(final String pathString,final boolean slashed){
        int start = startPositionWithoutWindowsDrive(pathString);
        return start>0 && pathString.length()>start
                &&((pathString.charAt(start)==SEPARATOR_CHAR)
        ||(pathString.charAt(start)=='\\'));
    }
    public FileSystem getFileSystem(Configuration conf)throws IOException{
        return FileSystem.get(this.toUri(),conf);
    }
    @Override
    public void validateObject() throws InvalidObjectException {

    }

    @Override
    public int compareTo(Path o) {
        return 0;
    }
}
