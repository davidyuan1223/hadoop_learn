package org.apache.hadoop.fs.permission;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PermissionParser {
    protected boolean symbolic=false;
    protected short userMode;
    protected short groupMode;
    protected short othersMode;
    protected short stickyMode;
    protected char userType='+';
    protected char groupType='+';
    protected char othersType='+';
    protected char stickyBitType='+';
    public PermissionParser(String modeStr, Pattern symbolic,Pattern octal)throws IllegalArgumentException{
        Matcher matcher=null;
        if ((matcher = symbolic.matcher(modeStr)).find()) {
            applyNormalPattern(modeStr,matcher);
        } else if ((matcher = octal.matcher(modeStr)).matches()) {
            applyOctalPattern(matcher);
        }else {
            throw new IllegalArgumentException(modeStr);
        }
    }
    private void applyNormalPattern(String modeStr, Matcher matcher){
        boolean commaSeparator=false;
        for (int i = 0; i < 1 || matcher.end() < modeStr.length(); i++) {
            if (i > 0 && (!commaSeparator || !matcher.find())) {
                throw new IllegalArgumentException(modeStr);
            }
            String str=matcher.group(2);
            char type=str.charAt(str.length()-1);
            boolean user,group,others,stickyBit;
            user=group=others=stickyBit=false;
            for (char c: matcher.group(1).toCharArray()){
                switch (c){
                    case 'u':
                        user=true;
                        break;
                    case 'g':
                        group=true;
                        break;
                    case 'o':
                        others=true;
                        break;
                    case 'a':
                        break;
                    default:
                        throw new RuntimeException("Unexpected");
                }
            }
            if (!(user || group || others)) {
                user=group=others=true;
            }
            short mode=0;
            for (char c : matcher.group(3).toCharArray()) {
                switch (c){
                    case 'r':
                        mode|=4;
                        break;
                    case 'w':
                        mode|=2;
                        break;
                    case 'x':
                        mode |=1;
                        break;
                    case 'X':
                        mode|=8;
                        break;
                    case 't':
                        stickyBit=true;
                        break;
                    default:
                        throw new RuntimeException("Unexpected");
                }
            }
            if (user) {
                userMode=mode;
                userType=type;
            }
            if (group) {
                groupMode=mode;
                groupType=type;
            }
            if (others) {
                othersMode=mode;
                othersType=type;
                stickyMode=(short)(stickyBit?1:0);
                stickyBitType=type;
            }
            commaSeparator=matcher.group(4).contains(",");
        }
        symbolic=true;
    }

    private void applyOctalPattern(final Matcher matcher){
        final char typeApply='=';
        stickyBitType=typeApply;
        userType=typeApply;
        groupType=typeApply;
        othersType=typeApply;
        String sb = matcher.group(1);
        if (!sb.isEmpty()) {
            stickyMode=Short.valueOf(sb.substring(0,1));
        }else {
            stickyMode=0;
        }
        String str = matcher.group(2);
        userMode=Short.valueOf(str.substring(0,1));
        groupMode=Short.valueOf(str.substring(1,2));
        othersMode=Short.valueOf(str.substring(2,3));
    }

    protected int combineModes(int existing, boolean exeOk){
        return combineModeSegments(stickyBitType,stickyMode,(existing>>>9),false)<<9 |
                combineModeSegments(userType,userMode,(existing>>>6)&7,exeOk) <<6 |
                combineModeSegments(groupType,groupMode,(existing>>>3)&7,exeOk)<<3 |
                combineModeSegments(othersType,othersMode,existing&7,exeOk);
    }

    protected int combineModeSegments(char type,int mode,int existing,boolean execOk){
        boolean capX=false;
        if ((mode&8)!=0){
            capX=true;
            mode&=~8;
            mode|=1;
        }
        switch (type){
            case '+': mode=mode|existing;break;
            case '-': mode=(~mode)&existing;break;
            case '=': break;
            default: throw new RuntimeException("Unexpected");
        }

        if (capX && !execOk && (mode&1) !=0 &&(existing&1)==0){
            mode&=~1;
        }
        return mode;
    }
}
