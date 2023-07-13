package org.apache.hadoop.fs.shell;

import java.util.*;

/**
 * Parse the args of a command and check the format of args
 */
public class CommandFormat {
    final int minPar,maxPar;
    final Map<String ,Boolean> options=new HashMap<>();
    final Map<String ,String > optionsWithValue=new HashMap<>();
    boolean ignoreUnknownOpts=false;
    @Deprecated
    public CommandFormat(String name,int min,int max,String ... possibleOpt){this(min,max,possibleOpt);}

    /**
     * simple parsing of command line arguments
     * @param min minimum arguments required
     * @param max maximum arguments permitted
     * @param possibleOpt list of the allowed switches
     */
    public CommandFormat(int min,int max, String ... possibleOpt){
        minPar=min;
        maxPar=max;
        for (String opt : possibleOpt) {
            if (opt == null) {
                ignoreUnknownOpts=true;
            }else {
                options.put(opt,Boolean.FALSE);
            }
        }
    }

    public void addOptionWithValue(String option){
        if (options.containsKey(option)) {
            throw new DuplicatedOptionException(option);
        }
        optionsWithValue.put(option,null);
    }

    public List<String> parse(String[] args,int pos){
        List<String > parameters=new ArrayList<>(Arrays.asList(args));
        parameters.subList(0,pos).clear();
        parse(parameters);
        return parameters;
    }

    public void parse(List<String > args){
        int pos=0;
        while (pos < args.size()) {
            String arg = args.get(pos);
            // stop if not an opt, or the stdin arg "-" is found
            if (!arg.startsWith("-") || arg.equals("-")) {
                break;
            } else if (arg.equals("--")) {
                // force end of option processing
                args.remove(pos);
                break;
            }
            // remove "-" start with the options
            String opt = arg.substring(1);
            if (options.containsKey(opt)) {
                args.remove(pos);
                options.put(opt,Boolean.TRUE);
            } else if (optionsWithValue.containsKey(opt)) {
                args.remove(pos);
                if (pos<args.size()&&(args.size()>minPar)&&!args.get(pos).startsWith("-")){
                    arg=args.get(pos);
                    args.remove(pos);
                }else {
                    arg="";
                }
                if (!arg.startsWith("-") || arg.equals("-")) {
                    optionsWithValue.put(opt,arg);
                }
            } else if (ignoreUnknownOpts) {
                pos++;
            }else {
                throw new UnknownOptionException(arg);
            }
        }
        int psize = args.size();
        if (psize < minPar) {
            throw new NotEnoughArgumentsException(minPar,psize);
        }
        if (psize > maxPar) {
            throw new TooManyArgumentsException(maxPar,psize);
        }
    }
    public boolean getOpt(String option){
        return options.getOrDefault(option, false);
    }
    public String getOptValue(String option){return optionsWithValue.get(option);}
    public Set<String > getOpts(){
        Set<String > optSet=new HashSet<>();
        for (Map.Entry<String, Boolean> entry : options.entrySet()) {
            if (entry.getValue()) {
                optSet.add(entry.getKey());
            }
        }
        return optSet;
    }
    public static abstract class IllegalNumberOfArgumentsException extends IllegalArgumentException{
        private static final long serialVersionUID=0L;
        protected int expected;
        protected int actual;
        protected IllegalNumberOfArgumentsException(int want,int got){
            expected=want;
            actual=got;
        }

        @Override
        public String getMessage() {
            return "expected "+expected+" but got "+actual;
        }
    }
    public static class DuplicatedOptionException extends IllegalArgumentException{
        private static final long serialVersionUID=0L;

        public DuplicatedOptionException(String duplicatedOption){
            super("option "+duplicatedOption+" already exists");
        }
    }
    public static class UnknownOptionException extends IllegalArgumentException{
        private static final long serialVersionUID=0L;
        protected String option=null;
        public UnknownOptionException(String unknownOption){
            super("Illegal option "+unknownOption);
            option=unknownOption;
        }
        public String getOption(){return option;}
    }
    public static class NotEnoughArgumentsException extends IllegalNumberOfArgumentsException{
        private static final long serialVersionUID=0L;
        public NotEnoughArgumentsException(int expected,int actual){
            super(expected,actual);
        }

        @Override
        public String getMessage() {
            return "Not enough arguments: "+super.getMessage();
        }
    }
    public static class TooManyArgumentsException extends IllegalNumberOfArgumentsException{
        private static final long serialVersionUID=0L;
        public TooManyArgumentsException(int expected,int actual){super(expected,actual);}

        @Override
        public String getMessage() {
            return "Too many arguments: "+super.getMessage();
        }
    }
}
