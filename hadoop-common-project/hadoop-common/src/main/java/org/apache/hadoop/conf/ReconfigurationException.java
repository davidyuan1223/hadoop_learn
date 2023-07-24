package org.apache.hadoop.conf;

/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/22
 **/
public class ReconfigurationException extends Exception {
    private static final long serialVersionUID=1L;
    private String property;
    private String newVal;
    private String oldVal;
    private static String constructMessage(String property,
                                           String newVal,
                                           String oldVal){
        String message="Could not change property "+property;
        if (oldVal != null) {
            message+=" from \'"+oldVal;
        }
        if (newVal != null) {
            message+="\' to \'"+newVal+"\'";
        }
        return message;
    }
    public ReconfigurationException(){
        super("Could not change configuration.");
        this.property=null;
        this.newVal=null;
        this.oldVal=null;
    }
    public ReconfigurationException(String property,
                                    String newVal,
                                    String oldVal,
                                    Throwable cause){
        super(constructMessage(property,newVal,oldVal),cause);
        this.property=property;
        this.oldVal=oldVal;
        this.newVal=newVal;
    }
    public ReconfigurationException(String property,
                                    String newVal,
                                    String oldVal){
        super(constructMessage(property,newVal,oldVal));
        this.property=property;
        this.newVal=newVal;
        this.oldVal=oldVal;
    }

    public String getProperty() {
        return property;
    }

    public String getNewVal() {
        return newVal;
    }

    public String getOldVal() {
        return oldVal;
    }
}
