package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.avro.reflect.Stringable;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputValidation;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/29
 **/
@Stringable
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Path implements Comparable<Path>, Serializable, ObjectInputValidation {
    public static final String SEPARATOR="/";
    public static final char SEPARATOR_CHAR='/';
    public static final String CUR_DIR=".";
    public static final boolean WINDOWS=System.getProperty("os.name").startsWith("Windows");
    private static final Pattern HAS_DRIVE_LETTER_SPECIFIER=Pattern.compile("^/?[a-zA-Z]:");
    private static final Pattern SLASHES=Pattern.compile("/+");
    private static final long serialVersionUID=0xad00f;
    private URI uri;

    void checkNotSchemeWithRelative(){
        if (toUri().isAbsolute() && !isUriPathAbsolute()){
            throw new HadoopIllegalArgumentException(
                    "Unsupported name: has scheme but relative path-part"
            );
        }
    }

    void checkNotRelative(){
        if (!isAbsolute() && toUri().getScheme()==null){
            throw new HadoopIllegalArgumentException("Path is relative");
        }
    }

    public static Path getPathWithoutSchemeAndAuthority(Path path){
        Path newPath=path.isUriPathAbsolute()?
                new Path(null,null,path.toUri().toPath()):
                path;
        return newPath;
    }

    public Path(String parent,String child){
        this(new Path(parent),new Path(child));
    }

    public Path(Path parent,String child){
        this(parent,new Path(child));
    }

    public Path(String parent,Path child){
        this(new Path(parent),child);
    }

    public Path(Path parent, Path child){
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

    private void checkPathArg(String path)throws IllegalArgumentException{
        if (path == null) {
            throw new IllegalArgumentException(
                    "Can not create a Path from a null string"
            );
        }
        if (path.length()==0) {
            throw new IllegalArgumentException(
                    "Can not create a Path from an empty string"
            );
        }
    }

    public Path(String pathString)throws IllegalArgumentException{
        checkPathArg(pathString);
        if (hasWindowsDrive(pathString) && pathString.charAt(0) != '/') {
            pathString="/"+pathString;
        }
        String scheme=null;
        String authority=null;
        int start=0;
        int colon=pathString.indexOf(':');
        int slash=pathString.indexOf('/');
        if ((colon != -1) && ((slash != -1) || (colon < slash))) {
            scheme=pathString.substring(0,colon);
            start=colon+1;
        }

        if (pathString.startsWith("//",start) &&
        (pathString.length()-start>2)){
            int nextSlash=pathString.indexOf('/',start+2);
            int authEnd=nextSlash>0?nextSlash:pathString.length();
            authority=pathString.substring(start+2,authEnd);
            start=authEnd;
        }
        String path = pathString.substring(start, pathString.length());
        initialize(scheme,authority,path,null);
    }

    public Path(URI aUri){
        uri=aUri.normalize();
    }

    public Path(String scheme,String authority,String path){
        checkPathArg(path);
        if (hasWindowsDrive(path) && path.charAt(0)!='/'){
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

    public static Path mergePaths(Path path1,Path path2){
        String path2Str=path2.toUri().getPath();
        path2Str-path2Str.substring(startPositionWithoutWindowsDrive(path2Str));
        return new Path(path1.toUri().getScheme(),
                path1.toUri().getAuthority(),
                path1.toUri().getPath()+path2Str);
    }

    private static String normalizePath(String scheme,String path){
        path=SLASHES.matcher(path).replaceAll("/");
        if (WINDOWS &&
                (hasWindowsDrive(path) ||
                        (scheme == null) ||
                        (scheme.isEmpty()) ||
                        (scheme.equals("file")))) {
            path= StringUtils.replace(path,"\\","/");
        }

        int minLength=startPositionWithoutWindowsDrive(path)+1;
        if (path.length() > minLength && path.endsWith(SEPARATOR)) {
            path=path.substring(0,path.length()-1);
        }
        return path;
    }

    private static boolean hasWindowsDrive(String path){
        return WINDOWS&&HAS_DRIVE_LETTER_SPECIFIER.matcher(path).find();
    }

    private static int startPositionWithoutWindowsDrive(String path){
        if (hasWindowsDrive(path)) {
            return path.charAt(0)==SEPARATOR_CHAR?3:2;
        }else {
            return 0;
        }
    }

    public static boolean isWindowsAbsolutePath(final String pathString,
                                                final boolean slashed){
        int start = startPositionWithoutWindowsDrive(pathString);
        return start>0
                && pathString.length()>start
                && ((pathString.charAt(start)==SEPARATOR_CHAR)
        ||(pathString.charAt(start)=='\\'));
    }

    public URI toUri(){
        return uri;
    }

    public FileSystem getFileSystem(Configuration conf)throws IOException{
        return FileSystem.get(this.toUri(),conf);
    }

    public boolean isAbsoluteAndSchemeAuthorityNull(){
        return (isUriPathAbsolute() &&
                uri.getScheme()==null && uri.getAuthority()==null);
    }

    public boolean isUriPathAbsolute(){
        int statr = startPositionWithoutWindowsDrive(uri.getPath());
        return uri.getPath().startsWith(SEPARATOR,statr);
    }

    public boolean isAbsolute(){
        return isUriPathAbsolute();
    }

    public boolean isRoot(){
        return getParent()==null;
    }

    public String getName(){
        String path = uri.getPath();
        int slash = path.lastIndexOf(SEPARATOR);
        return path.substring(slash+1);
    }

    public Path getParent(){
        return getParentUtil();
    }

    public Optional<Path> getOptionalParentPath(){
        return Optional.ofNullable(getParentUtil());
    }

    private Path getParentUtil(){
        String path = uri.getPath();
        int lashSlash = path.lastIndexOf('/');
        int start = startPositionWithoutWindowsDrive(path);
        if (path.length() == start || (lashSlash == start && path.length() == start + 1)) {
            return null;
        }
        String parent;
        if (lashSlash==-1) {
            parent=CUR_DIR;
        }else {
            parent=path.substring(0,lashSlash==start?start+1:lashSlash);
        }
        return new Path(uri.getScheme(),uri.getAuthority(),parent);
    }

    public Path suffix(String suffix){
        return new Path(getParent(),getName()+suffix);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (uri.getScheme() != null) {
            sb.append(uri.getScheme())
                    .append(":");
        }
        if (uri.getAuthority() != null) {
            sb.append("//")
                    .append(uri.getAuthority());
        }
        if (uri.getPath() != null) {
            String path = uri.getPath();
            if (path.indexOf('/') == 0
                    && hasWindowsDrive(path)
                    && uri.getScheme() == null
                    && uri.getAuthority() == null) {
                path=path.substring(1);
            }
            sb.append(path);
        }
        if (uri.getFragment() != null) {
            sb.append("#")
                    .append(uri.getFragment());
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Path)) {
            return false;
        }
        return this.uri.equals(((Path)o).uri);
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    @Override
    public int compareTo(Path o) {
        return this.uri.compareTo(o.uri);
    }

    public int depth(){
        String path = uri.getPath();
        int depth=0;
        int slash=path.length()==1&&path.charAt(0)=='/'?-1:0;
        while (slash != -1) {
            depth++;
            slash=path.indexOf(SEPARATOR,slash+1);
        }
        return depth;
    }

    @Deprecated
    public Path makeQualified(FileSystem fs){
        return makeQualified(fs.getUri(),fs.getWorkingDirectory());
    }

    @InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
    public Path makeQualified(URI defaultUri,Path workingDir){
        Path path=this;
        if (!isAbsolute()) {
            path=new Path(workingDir,this);
        }
        URI pathUri = path.toUri();
        String scheme = pathUri.getScheme();
        String authority = pathUri.getAuthority();
        String fragment = pathUri.getFragment();

        if (scheme != null
                && (authority != null || defaultUri.getAuthority() == null)) {
            return path;
        }

        if (scheme == null) {
            scheme=defaultUri.getScheme();
        }
        if (authority == null) {
            authority=defaultUri.getAuthority();
            if (authority == null) {
                authority="";
            }
        }
        URI newUri=null;
        try {
            newUri= new URI(scheme,authority,
                    normalizePath(scheme,pathUri.getPath()), null, fragment);
        }catch (URISyntaxException e){
            throw new IllegalArgumentException(e);
        }
        return new Path(newUri);
    }

    @Override
    public void validateObject() throws InvalidObjectException {
        if (uri == null) {
            throw new InvalidObjectException("No URI in deserialized Path");
        }
    }
}
