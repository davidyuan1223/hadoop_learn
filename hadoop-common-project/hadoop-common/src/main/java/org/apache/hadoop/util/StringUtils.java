package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.thirdparty.com.google.common.net.InetAddresses;
import org.apache.log4j.LogManager;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class StringUtils {
    public static final int SHUTDOWN_HOOK_PRIORITY=0;
    public static final Pattern SHELL_ENV_VAR_PATTERN=Pattern.compile("\\$([A-Za-z_]{1}[A-Za-z0-9_]*)");
    public static final Pattern WIN_EV_VAR_PATTERN=Pattern.compile("%(.*?)%");
    public static final Pattern ENV_VAR_PATTERN=Shell.WINDOWS?WIN_EV_VAR_PATTERN:SHELL_ENV_VAR_PATTERN;
    public static String stringifyException(Throwable e){
        StringWriter stm = new StringWriter();
        PrintWriter wrt = new PrintWriter(stm);
        e.printStackTrace();
        wrt.close();
        return stm.toString();
    }
    public static String simpleHostname(String fullHostname){
        if (InetAddresses.isInetAddress(fullHostname)) {
            return fullHostname;
        }
        int offset = fullHostname.indexOf('.');
        if (offset!=-1) {
            return fullHostname.substring(0,offset);
        }
        return fullHostname;
    }
    @Deprecated
    public static String humanReadableInt(long number){
        return TraditionBinaryPrefix.long2String(number,"",1);
    }
    public static String format(final String format,final Object... objects){
        return String.format(Locale.ENGLISH,format,objects);
    }
    public static String formatPercent(double fraction,int decimalPlaces){
        return format("%."+decimalPlaces+"f%%",fraction*100);
    }
    public static String arrayToString(String[] strs){
        if (strs.length==0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(strs[0]);
        for (int i = 1; i < strs.length; i++) {
            sb.append(",");
            sb.append(strs[i]);
        }
        return sb.toString();
    }
    public static String byteToHexString(byte[] bytes,int start,int end){
        if (bytes == null) {
            throw new IllegalArgumentException("bytes==null");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++) {
            sb.append(format("%02x",bytes[i]));
        }
        return sb.toString();
    }
    public static String byteToHexString(byte[] bytes){
        return byteToHexString(bytes,0,bytes.length);
    }
    public static String byteToHexString(byte b){
        return byteToHexString(new byte[]{b});
    }
    public static byte[] hexStringToBytes(String hex){
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i]=(byte) Integer.parseInt(hex.substring(2*i,2*i+2),16);
        }
        return bytes;
    }
    public static String uriToString(URI[] uris){
        if (uris == null) {
            return null;
        }
        StringBuilder ret = new StringBuilder(uris[0].toString());
        for (int i = 1; i < uris.length; i++) {
            ret.append(",");
            ret.append(uris[i].toString());
        }
        return ret.toString();
    }
    public static URI[] stringToURI(String[] strs){
        if (strs == null) {
            return null;
        }
        URI[] uris = new URI[strs.length];
        for (int i = 0; i < strs.length; i++) {
            try {
                uris[i]=new URI(strs[i]);
            }catch (URISyntaxException e){
                throw new IllegalArgumentException("Failed to create uri for "+strs[i],e);
            }
        }
        return uris;
    }
    public static Path[] stringToPath(String[] str){
        if (str == null) {
            return null;
        }
        Path[] paths = new Path[str.length];
        for (int i = 0; i < str.length; i++) {
            paths[i]=new Path(str[i]);
        }
        return paths;
    }

    public static String formatTimeDiff(long finishTime,long startTime){
        long timeDiff=finishTime-startTime;
        return formatTime(timeDiff);
    }
    public static String formatTime(long timeDiff){
        StringBuilder sb = new StringBuilder();
        long hours=timeDiff/(60*60*1000);
        long rem=(timeDiff%(60*60*1000));
        long minutes=rem/(60*1000);
        rem=rem%(60*1000);
        long seconds=rem/1000;
        if (hours!=0) {
            sb.append(hours)
                    .append("hrs, ");
        }
        if (minutes!=0) {
            sb.append(minutes)
                    .append("mins, ");
        }
        sb.append(seconds)
                .append("sec");
        return sb.toString();
    }
    public static String formatTimeSortable(long timeDiff){
        StringBuilder sb = new StringBuilder();
        long hours=timeDiff/(60*60*1000);
        long rem=(timeDiff%(60*60*1000));
        long minutes=rem/(60*1000);
        rem=rem%(60*1000);
        long seconds=rem/1000;
        if (hours > 99) {
            hours=99;
            minutes=59;
            seconds=59;
        }
        sb.append(String.format("%02d",hours))
                .append("hrs, ")
                .append(String.format("%02d",minutes))
                .append("mins, ")
                .append(String.format("%02d",seconds))
                .append("sec");
        return sb.toString();
    }
    public static String getFormattedTimeWithDiff(FastDateFormat dateFormat,long finishTime,long startTime){
        String formattedFinishTime = dateFormat.format(finishTime);
        return getFormattedTimeWithDiff(formattedFinishTime,finishTime,startTime);
    }
    public static String getFormattedTimeWithDiff(String formattedFinishTime,long finishTime,long startTime){
        StringBuilder sb = new StringBuilder();
        if (0 != finishTime) {
            sb.append(formattedFinishTime);
            if (0 != startTime) {
                sb.append(" ("+formatTimeDiff(finishTime,startTime)+")");
            }
        }
        return sb.toString();
    }
    public static String[] getStrings(String str){
        String delim=",";
        return getStrings(str,delim);
    }
    public static String[] getStrings(String str,String delim){
        Collection<String > values=getStringCollection(str,delim);
        if (values.size()==0) {
            return null;
        }
        return values.toArray(new String[values.size()]);
    }
    public static Collection<String > getStringCollection(String str){
        String delim=",";
        return getStringCollection(str,delim);
    }
    public static Collection<String > getStringCollection(String str,String delim){
        List<String > values=new ArrayList<>();
        if (str == null) {
            return values;
        }
        StringTokenizer tokenizer = new StringTokenizer(str, delim);
        while (tokenizer.hasMoreTokens()) {
            values.add(tokenizer.nextToken());
        }
        return values;
    }
    public static Collection<String > getTrimmedStringCollection(String str,String delim){
        List<String > values=new ArrayList<>();
        if (str == null) {
            return values;
        }
        StringTokenizer tokenizer = new StringTokenizer(str, delim);
        while (tokenizer.hasMoreTokens()) {
            String next = tokenizer.nextToken();
            if (next == null || next.trim().isEmpty()) {
                continue;
            }
            values.add(next.trim());
        }
        return values;
    }
    public static Collection<String > getTrimmedStringCollection(String str){
        Set<String > set=new LinkedHashSet<String >(Arrays.asList(getTrimmedStrings(str)));
        set.remove("");
        return set;
    }
    public static String[] getTrimmedStrings(String str){
        if (null == str || str.trim().isEmpty()) {
            return emptyStringArray;
        }
        return str.trim().split("\\s*[,\n]\\s*");
    }
    public static final String[] emptyStringArray={};
    public static final char COMMA=',';
    public static final String COMMA_STR=",";
    public static final char ESCAPE_CHAR='\\';
    public static String[] split(String str){
        return split(str,ESCAPE_CHAR,COMMA);
    }
    public static String[] split(String str,char escapeChar,char separator){
        if (str == null) {
            return null;
        }
        ArrayList<String > strList=new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int index=0;
        while ((index = findNext(str, separator, escapeChar, index, sb)) >= 0) {
            ++index;
            strList.add(sb.toString());
            sb.setLength(0);
        }
        strList.add(sb.toString());
        int last=strList.size();
        while (--last >= 0 && "".equals(strList.get(last))) {
            strList.remove(last);
        }
        return strList.toArray(new String[strList.size()]);
    }
    public static String[] split(
            String str, char separator) {
        // String.split returns a single empty result for splitting the empty
        // string.
        if (str.isEmpty()) {
            return new String[]{""};
        }
        ArrayList<String> strList = new ArrayList<String>();
        int startIndex = 0;
        int nextIndex = 0;
        while ((nextIndex = str.indexOf(separator, startIndex)) != -1) {
            strList.add(str.substring(startIndex, nextIndex));
            startIndex = nextIndex + 1;
        }
        strList.add(str.substring(startIndex));
        // remove trailing empty split(s)
        int last = strList.size(); // last split
        while (--last>=0 && "".equals(strList.get(last))) {
            strList.remove(last);
        }
        return strList.toArray(new String[strList.size()]);
    }
    public static int findNext(String str, char separator, char escapeChar,
                               int start, StringBuilder split) {
        int numPreEscapes = 0;
        for (int i = start; i < str.length(); i++) {
            char curChar = str.charAt(i);
            if (numPreEscapes == 0 && curChar == separator) { // separator
                return i;
            } else {
                split.append(curChar);
                numPreEscapes = (curChar == escapeChar)
                        ? (++numPreEscapes) % 2
                        : 0;
            }
        }
        return -1;
    }
    public static String escapeString(String str) {
        return escapeString(str, ESCAPE_CHAR, COMMA);
    }

    /**
     * Escape <code>charToEscape</code> in the string
     * with the escape char <code>escapeChar</code>
     *
     * @param str string
     * @param escapeChar escape char
     * @param charToEscape the char to be escaped
     * @return an escaped string
     */
    public static String escapeString(
            String str, char escapeChar, char charToEscape) {
        return escapeString(str, escapeChar, new char[] {charToEscape});
    }

    // check if the character array has the character
    private static boolean hasChar(char[] chars, char character) {
        for (char target : chars) {
            if (character == target) {
                return true;
            }
        }
        return false;
    }

    /**
     * escapeString.
     *
     * @param str str.
     * @param escapeChar escapeChar.
     * @param charsToEscape array of characters to be escaped
     * @return escapeString.
     */
    public static String escapeString(String str, char escapeChar,
                                      char[] charsToEscape) {
        if (str == null) {
            return null;
        }
        StringBuilder result = new StringBuilder();
        for (int i=0; i<str.length(); i++) {
            char curChar = str.charAt(i);
            if (curChar == escapeChar || hasChar(charsToEscape, curChar)) {
                // special char
                result.append(escapeChar);
            }
            result.append(curChar);
        }
        return result.toString();
    }

    /**
     * Unescape commas in the string using the default escape char
     * @param str a string
     * @return an unescaped string
     */
    public static String unEscapeString(String str) {
        return unEscapeString(str, ESCAPE_CHAR, COMMA);
    }

    /**
     * Unescape <code>charToEscape</code> in the string
     * with the escape char <code>escapeChar</code>
     *
     * @param str string
     * @param escapeChar escape char
     * @param charToEscape the escaped char
     * @return an unescaped string
     */
    public static String unEscapeString(
            String str, char escapeChar, char charToEscape) {
        return unEscapeString(str, escapeChar, new char[] {charToEscape});
    }

    /**
     * unEscapeString.
     * @param str str.
     * @param escapeChar escapeChar.
     * @param charsToEscape array of characters to unescape
     * @return escape string.
     */
    public static String unEscapeString(String str, char escapeChar,
                                        char[] charsToEscape) {
        if (str == null) {
            return null;
        }
        StringBuilder result = new StringBuilder(str.length());
        boolean hasPreEscape = false;
        for (int i=0; i<str.length(); i++) {
            char curChar = str.charAt(i);
            if (hasPreEscape) {
                if (curChar != escapeChar && !hasChar(charsToEscape, curChar)) {
                    // no special char
                    throw new IllegalArgumentException("Illegal escaped string " + str +
                            " unescaped " + escapeChar + " at " + (i-1));
                }
                // otherwise discard the escape char
                result.append(curChar);
                hasPreEscape = false;
            } else {
                if (hasChar(charsToEscape, curChar)) {
                    throw new IllegalArgumentException("Illegal escaped string " + str +
                            " unescaped " + curChar + " at " + i);
                } else if (curChar == escapeChar) {
                    hasPreEscape = true;
                } else {
                    result.append(curChar);
                }
            }
        }
        if (hasPreEscape ) {
            throw new IllegalArgumentException("Illegal escaped string " + str +
                    ", not expecting " + escapeChar + " in the end." );
        }
        return result.toString();
    }

    /**
     * Return a message for logging.
     * @param prefix prefix keyword for the message
     * @param msg content of the message
     * @return a message for logging
     */
    public static String toStartupShutdownString(String prefix, String[] msg) {
        StringBuilder b = new StringBuilder(prefix);
        b.append("\n/************************************************************");
        for(String s : msg)
            b.append("\n").append(prefix).append(s);
        b.append("\n************************************************************/");
        return b.toString();
    }

    /**
     * Print a log message for starting up and shutting down
     * @param clazz the class of the server
     * @param args arguments
     * @param LOG the target log object
     */
    public static void startupShutdownMessage(Class<?> clazz, String[] args,
                                              final org.apache.commons.logging.Log LOG) {
        startupShutdownMessage(clazz, args, LogAdapter.create(LOG));
    }

    /**
     * Print a log message for starting up and shutting down
     * @param clazz the class of the server
     * @param args arguments
     * @param LOG the target log object
     */
    public static void startupShutdownMessage(Class<?> clazz, String[] args,
                                              final org.slf4j.Logger LOG) {
        startupShutdownMessage(clazz, args, LogAdapter.create(LOG));
    }

    static void startupShutdownMessage(Class<?> clazz, String[] args,
                                       final LogAdapter LOG) {
        final String hostname = NetUtils.getHostname();
        final String classname = clazz.getSimpleName();
        LOG.info(createStartupShutdownMessage(classname, hostname, args));

        if (SystemUtils.IS_OS_UNIX) {
            try {
                SignalLogger.INSTANCE.register(LOG);
            } catch (Throwable t) {
                LOG.warn("failed to register any UNIX signal loggers: ", t);
            }
        }
        ShutdownHookManager.get().addShutdownHook(
                new Runnable() {
                    @Override
                    public void run() {
                        LOG.info(toStartupShutdownString("SHUTDOWN_MSG: ", new String[]{
                                "Shutting down " + classname + " at " + hostname}));
                        LogManager.shutdown();
                    }
                }, SHUTDOWN_HOOK_PRIORITY);

    }

    /**
     * Generate the text for the startup/shutdown message of processes.
     * @param classname short classname of the class
     * @param hostname hostname
     * @param args Command arguments
     * @return a string to log.
     */
    public static String createStartupShutdownMessage(String classname,
                                                      String hostname, String[] args) {
        return toStartupShutdownString("STARTUP_MSG: ", new String[] {
                "Starting " + classname,
                "  host = " + hostname,
                "  args = " + (args != null ? Arrays.asList(args) : new ArrayList<>()),
                "  version = " + VersionInfo.getVersion(),
                "  classpath = " + System.getProperty("java.class.path"),
                "  build = " + VersionInfo.getUrl() + " -r "
                        + VersionInfo.getRevision()
                        + "; compiled by '" + VersionInfo.getUser()
                        + "' on " + VersionInfo.getDate(),
                "  java = " + System.getProperty("java.version") }
        );
    }

    public static String escapeHTML(String string) {
        if(string == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        boolean lastCharacterWasSpace = false;
        char[] chars = string.toCharArray();
        for(char c : chars) {
            if(c == ' ') {
                if(lastCharacterWasSpace){
                    lastCharacterWasSpace = false;
                    sb.append("&nbsp;");
                }else {
                    lastCharacterWasSpace=true;
                    sb.append(" ");
                }
            }else {
                lastCharacterWasSpace = false;
                switch(c) {
                    case '<': sb.append("&lt;"); break;
                    case '>': sb.append("&gt;"); break;
                    case '&': sb.append("&amp;"); break;
                    case '"': sb.append("&quot;"); break;
                    default : sb.append(c);break;
                }
            }
        }

        return sb.toString();
    }

    /**
     * a byte description of the given long interger value.
     *
     * @param len len.
     * @return a byte description of the given long interger value.
     */
    public static String byteDesc(long len) {
        return TraditionalBinaryPrefix.long2String(len, "B", 2);
    }

    /**
     * limitDecimalTo2.
     *
     * @param d double param.
     * @return string value ("%.2f").
     * @deprecated use StringUtils.format("%.2f", d).
     */
    @Deprecated
    public static String limitDecimalTo2(double d) {
        return format("%.2f", d);
    }

    /**
     * Concatenates strings, using a separator.
     *
     * @param separator Separator to join with.
     * @param strings Strings to join.
     * @return join string.
     */
    public static String join(CharSequence separator, Iterable<?> strings) {
        Iterator<?> i = strings.iterator();
        if (!i.hasNext()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(i.next().toString());
        while (i.hasNext()) {
            sb.append(separator);
            sb.append(i.next().toString());
        }
        return sb.toString();
    }

    public static String join(char separator, Iterable<?> strings) {
        return join(separator + "", strings);
    }

    /**
     * Concatenates strings, using a separator.
     *
     * @param separator to join with
     * @param strings to join
     * @return  the joined string
     */
    public static String join(CharSequence separator, String[] strings) {
        // Ideally we don't have to duplicate the code here if array is iterable.
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String s : strings) {
            if (first) {
                first = false;
            } else {
                sb.append(separator);
            }
            sb.append(s);
        }
        return sb.toString();
    }

    public static String join(char separator, String[] strings) {
        return join(separator + "", strings);
    }

    /**
     * Convert SOME_STUFF to SomeStuff
     *
     * @param s input string
     * @return camelized string
     */
    public static String camelize(String s) {
        StringBuilder sb = new StringBuilder();
        String[] words = split(StringUtils.toLowerCase(s), ESCAPE_CHAR,  '_');

        for (String word : words)
            sb.append(org.apache.commons.lang3.StringUtils.capitalize(word));

        return sb.toString();
    }

    /**
     * Matches a template string against a pattern, replaces matched tokens with
     * the supplied replacements, and returns the result.  The regular expression
     * must use a capturing group.  The value of the first capturing group is used
     * to look up the replacement.  If no replacement is found for the token, then
     * it is replaced with the empty string.
     *
     * For example, assume template is "%foo%_%bar%_%baz%", pattern is "%(.*?)%",
     * and replacements contains 2 entries, mapping "foo" to "zoo" and "baz" to
     * "zaz".  The result returned would be "zoo__zaz".
     *
     * @param template String template to receive replacements
     * @param pattern Pattern to match for identifying tokens, must use a capturing
     *   group
     * @param replacements Map&lt;String, String&gt; mapping tokens identified by
     * the capturing group to their replacement values
     * @return String template with replacements
     */
    public static String replaceTokens(String template, Pattern pattern,
                                       Map<String, String> replacements) {
        StringBuffer sb = new StringBuffer();
        Matcher matcher = pattern.matcher(template);
        while (matcher.find()) {
            String replacement = replacements.get(matcher.group(1));
            if (replacement == null) {
                replacement = "";
            }
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * Get stack trace for a given thread.
     * @param t thread.
     * @return stack trace string.
     */
    public static String getStackTrace(Thread t) {
        final StackTraceElement[] stackTrace = t.getStackTrace();
        StringBuilder str = new StringBuilder();
        for (StackTraceElement e : stackTrace) {
            str.append(e.toString() + "\n");
        }
        return str.toString();
    }

    /**
     * From a list of command-line arguments, remove both an option and the
     * next argument.
     *
     * @param name  Name of the option to remove.  Example: -foo.
     * @param args  List of arguments.
     * @return      null if the option was not found; the value of the
     *              option otherwise.
     * @throws IllegalArgumentException if the option's argument is not present
     */
    public static String popOptionWithArgument(String name, List<String> args)
            throws IllegalArgumentException {
        String val = null;
        for (Iterator<String> iter = args.iterator(); iter.hasNext(); ) {
            String cur = iter.next();
            if (cur.equals("--")) {
                // stop parsing arguments when you see --
                break;
            } else if (cur.equals(name)) {
                iter.remove();
                if (!iter.hasNext()) {
                    throw new IllegalArgumentException("option " + name + " requires 1 " +
                            "argument.");
                }
                val = iter.next();
                iter.remove();
                break;
            }
        }
        return val;
    }

    /**
     * From a list of command-line arguments, remove an option.
     *
     * @param name  Name of the option to remove.  Example: -foo.
     * @param args  List of arguments.
     * @return      true if the option was found and removed; false otherwise.
     */
    public static boolean popOption(String name, List<String> args) {
        for (Iterator<String> iter = args.iterator(); iter.hasNext(); ) {
            String cur = iter.next();
            if (cur.equals("--")) {
                // stop parsing arguments when you see --
                break;
            } else if (cur.equals(name)) {
                iter.remove();
                return true;
            }
        }
        return false;
    }

    /**
     * From a list of command-line arguments, return the first non-option
     * argument.  Non-option arguments are those which either come after
     * a double dash (--) or do not start with a dash.
     *
     * @param args  List of arguments.
     * @return      The first non-option argument, or null if there were none.
     */
    public static String popFirstNonOption(List<String> args) {
        for (Iterator<String> iter = args.iterator(); iter.hasNext(); ) {
            String cur = iter.next();
            if (cur.equals("--")) {
                if (!iter.hasNext()) {
                    return null;
                }
                cur = iter.next();
                iter.remove();
                return cur;
            } else if (!cur.startsWith("-")) {
                iter.remove();
                return cur;
            }
        }
        return null;
    }

    /**
     * Converts all of the characters in this String to lower case with
     * Locale.ENGLISH.
     *
     * @param str  string to be converted
     * @return     the str, converted to lowercase.
     */
    public static String toLowerCase(String str) {
        return str.toLowerCase(Locale.ENGLISH);
    }

    /**
     * Converts all of the characters in this String to upper case with
     * Locale.ENGLISH.
     *
     * @param str  string to be converted
     * @return     the str, converted to uppercase.
     */
    public static String toUpperCase(String str) {
        return str.toUpperCase(Locale.ENGLISH);
    }

    /**
     * Compare strings locale-freely by using String#equalsIgnoreCase.
     *
     * @param s1  Non-null string to be converted
     * @param s2  string to be converted
     * @return     the str, converted to uppercase.
     */
    public static boolean equalsIgnoreCase(String s1, String s2) {
        Preconditions.checkNotNull(s1);
        // don't check non-null against s2 to make the semantics same as
        // s1.equals(s2)
        return s1.equalsIgnoreCase(s2);
    }

    /**
     * <p>Checks if the String contains only unicode letters.</p>
     *
     * <p><code>null</code> will return <code>false</code>.
     * An empty String (length()=0) will return <code>true</code>.</p>
     *
     * <pre>
     * StringUtils.isAlpha(null)   = false
     * StringUtils.isAlpha("")     = true
     * StringUtils.isAlpha("  ")   = false
     * StringUtils.isAlpha("abc")  = true
     * StringUtils.isAlpha("ab2c") = false
     * StringUtils.isAlpha("ab-c") = false
     * </pre>
     *
     * @param str  the String to check, may be null
     * @return <code>true</code> if only contains letters, and is non-null
     */
    public static boolean isAlpha(String str) {
        if (str == null) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if (!Character.isLetter(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Same as WordUtils#wrap in commons-lang 2.6. Unlike commons-lang3, leading
     * spaces on the first line are NOT stripped.
     *
     * @param str  the String to be word wrapped, may be null
     * @param wrapLength  the column to wrap the words at, less than 1 is treated
     *                   as 1
     * @param newLineStr  the string to insert for a new line,
     *  <code>null</code> uses the system property line separator
     * @param wrapLongWords  true if long words (such as URLs) should be wrapped
     * @return a line with newlines inserted, <code>null</code> if null input
     */
    public static String wrap(String str, int wrapLength, String newLineStr,
                              boolean wrapLongWords) {
        if(str == null) {
            return null;
        } else {
            if(newLineStr == null) {
                newLineStr = System.lineSeparator();
            }

            if(wrapLength < 1) {
                wrapLength = 1;
            }

            int inputLineLength = str.length();
            int offset = 0;
            StringBuffer wrappedLine = new StringBuffer(inputLineLength + 32);

            while(inputLineLength - offset > wrapLength) {
                if(str.charAt(offset) == 32) {
                    ++offset;
                } else {
                    int spaceToWrapAt = str.lastIndexOf(32, wrapLength + offset);
                    if(spaceToWrapAt >= offset) {
                        wrappedLine.append(str.substring(offset, spaceToWrapAt));
                        wrappedLine.append(newLineStr);
                        offset = spaceToWrapAt + 1;
                    } else if(wrapLongWords) {
                        wrappedLine.append(str.substring(offset, wrapLength + offset));
                        wrappedLine.append(newLineStr);
                        offset += wrapLength;
                    } else {
                        spaceToWrapAt = str.indexOf(32, wrapLength + offset);
                        if(spaceToWrapAt >= 0) {
                            wrappedLine.append(str.substring(offset, spaceToWrapAt));
                            wrappedLine.append(newLineStr);
                            offset = spaceToWrapAt + 1;
                        } else {
                            wrappedLine.append(str.substring(offset));
                            offset = inputLineLength;
                        }
                    }
                }
            }

            wrappedLine.append(str.substring(offset));
            return wrappedLine.toString();
        }
    }






    public enum TraditionBinaryPrefix{
        KILO(10),
        MEGA(KILO.bitShift+10),
        GIGA(MEGA.bitShift + 10),
        TERA(GIGA.bitShift + 10),
        PETA(TERA.bitShift + 10),
        EXA (PETA.bitShift + 10);

        public final long value;
        public final  char symbol;
        public final int bitShift;
        public final long bitMask;

        private TraditionBinaryPrefix(int bitShift){
            this.bitShift=bitShift;
            this.value=1L<< bitShift;
            this.bitMask=this.value-1L;
            this.symbol=toString().charAt(0);
        }

        public static TraditionBinaryPrefix valueOf(char symbol){
            symbol=Character.toUpperCase(symbol);
            for (TraditionBinaryPrefix prefix : TraditionBinaryPrefix.values()) {
                if (symbol == prefix.symbol) {
                    return prefix;
                }
            }
            throw new IllegalArgumentException("Unknown symbol '"+symbol+"'");
        }

        public static long string2long(String s){
            s=s.trim();
            final int lastpos=s.length()-1;
            final char lastchar=s.charAt(lastpos);
            if (Character.isDigit(lastchar)) {
                return Long.parseLong(s);
            }else {
                long prefix;
                try{
                    prefix=TraditionBinaryPrefix.valueOf(lastchar).value;
                }catch (IllegalArgumentException e){
                    throw new IllegalArgumentException("Invalid size prefix '"+lastchar+"' in '"+
                            s+"'. Allowed prefixes are k,m,g,t,p,e(case insensitive)");
                }
                long num=Long.parseLong(s.substring(0,lastpos));
                if (num > (Long.MAX_VALUE / prefix) || num < (Long.MIN_VALUE / prefix)) {
                    throw new IllegalArgumentException(s+" does not fit in a Long");
                }
                return num*prefix;
            }
        }
        public static String long2String(long n, String unit, int decimalPlaces) {
            if (unit == null) {
                unit = "";
            }
            //take care a special case
            if (n == Long.MIN_VALUE) {
                return "-8 " + EXA.symbol + unit;
            }

            final StringBuilder b = new StringBuilder();
            //take care negative numbers
            if (n < 0) {
                b.append('-');
                n = -n;
            }
            if (n < KILO.value) {
                //no prefix
                b.append(n);
                return (unit.isEmpty()? b: b.append(" ").append(unit)).toString();
            } else {
                //find traditional binary prefix
                int i = 0;
                for(; i < values().length && n >= values()[i].value; i++);
                TraditionalBinaryPrefix prefix = values()[i - 1];

                if ((n & prefix.bitMask) == 0) {
                    //exact division
                    b.append(n >> prefix.bitShift);
                } else {
                    final String  format = "%." + decimalPlaces + "f";
                    String s = format(format, n/(double)prefix.value);
                    //check a special rounding up case
                    if (s.startsWith("1024")) {
                        prefix = values()[i];
                        s = format(format, n/(double)prefix.value);
                    }
                    b.append(s);
                }
                return b.append(' ').append(prefix.symbol).append(unit).toString();
            }
        }

    }
}
