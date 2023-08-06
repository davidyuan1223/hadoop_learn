package org.apache.hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class FileBasedIPList implements IPList{
    private static final Logger LOG= LoggerFactory.getLogger(FileBasedIPList.class);
    private final String fileName;
    private final MachineList addressList;

    public FileBasedIPList(String fileName){
        this.fileName=fileName;
        String[] lines;
        try {
            lines=readLines(fileName);
        }catch (IOException e){
            lines=null;
        }
        if (lines != null) {
            addressList=new MachineList(new HashSet<>(Arrays.asList(lines)));
        }else {
            addressList=null;
        }
    }

    public FileBasedIPList reload(){
        return new FileBasedIPList(fileName);
    }

    @Override
    public boolean isIn(String ipAddress) {
        if (ipAddress == null || addressList == null) {
            return false;
        }
        return addressList.includes(ipAddress);
    }

    private static String[] readLines(String fileName)throws IOException{
        try {
            if (fileName != null) {
                File file = new File(fileName);
                if (file.exists()) {
                    try (
                            InputStreamReader fileReader = new InputStreamReader(Files.newInputStream(file.toPath()), StandardCharsets.UTF_8);
                            BufferedReader bufferedReader=new BufferedReader(fileReader)){
                        List<String > lines=new ArrayList<>();
                        String line=null;
                        while ((line = bufferedReader.readLine())!= null) {
                            lines.add(line);
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Loaded IP list of size = "+lines.size()+" from file = "+fileName);
                        }
                        return (lines.toArray(new String[lines.size()]));
                    }
                }else {
                    LOG.debug("Missing ip list file : "+fileName);
                }
            }
        }catch (IOException e){
            LOG.error(e.toString());
            throw e;
        }
        return null;
    }
}
