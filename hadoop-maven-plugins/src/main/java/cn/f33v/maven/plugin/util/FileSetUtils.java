package cn.f33v.maven.plugin.util;


import org.apache.maven.model.FileSet;
import org.codehaus.plexus.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * FileSetUtils 包含需要与maven fileSet一起使用的mojo实现的辅助方法
 */
public class FileSetUtils {
    /**
     * 返回包含给定list所有元素的字符串，用,对元素进行分割
     * @param list - 元素列表
     * @return - 包含所有元素且被,分割的字符串
     */
    private static String getCommaSeparatedList(List<String > list){
        StringBuilder stringBuilder = new StringBuilder();
        String separator="";
        for (Object e : list) {
            stringBuilder.append(separator)
                    .append(e);
            separator=",";
        }
        return stringBuilder.toString();
    }

    /**
     * 转换maven fileSet为file对象列表
     * @param source - 要转换的fileset
     * @return - 包含fileset并转换为file元素的列表
     * @throws IOException - io错误
     */
    @SuppressWarnings("unchecked")
    public static List<File> convertFileSetToFiles(FileSet source) throws IOException {
        String includes = getCommaSeparatedList(source.getIncludes());
        String excludes = getCommaSeparatedList(source.getExcludes());
        return FileUtils.getFiles(new File(source.getDirectory()),includes,excludes);
    }
}
