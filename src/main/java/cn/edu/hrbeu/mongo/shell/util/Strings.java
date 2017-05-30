/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hrbeu.mongo.shell.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wu on 2017/5/25.
 */
public class Strings {

    public static int toInteger(String s) {
        try {
            return Integer.parseInt(s);
        } catch (Exception ex) {
            return -1;
        }
    }
    
    public static String trimAll(String s) {
        if (s == null) {
            return "";
        }
        return s.replaceAll("\\s*", "");
    }

    // 严格为空，包括 null , "", 或 n个有不同形式的空格
    public static boolean isEmptyStrictly(String s) {
        return s == null || s.isEmpty() || trimAll(s).isEmpty();
    }

    // 是否单独且紧凑的字符串，非空，并且中间没有任何形式的空格
    public static boolean isCompactedWorld(String s) {
        return s != null && !s.isEmpty() && trimAll(s).equals(s);
    }

    public static boolean isEmail(String email) {
        return matched(email, "\\w+([-+.]\\w+)*@\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*");
    }

    public static boolean matched(String str, String format) {
        if (isEmptyStrictly(str)) {
            return false;
        }
        if (isEmptyStrictly(format)) {
            return false;
        }
        Pattern p = Pattern.compile(format);//复杂匹配  
        Matcher m = p.matcher(str);
        return m.matches();
    }

    public static boolean isUpperCaseChar(char c) {
        return (c >= 'A' && c <= 'Z');
    }

    public static boolean isLowerCaseChar(char c) {
        return (c >= 'a' && c <= 'z');
    }

    public static boolean isNumberChar(char c) {
        return (c >= '0' && c <= '9');
    }

    public static boolean isCommandSplitChar(char c) {
        return (c == '-' || c == '_');
    }

    // 获取指令的第一个英文字单词
    public static String[] firstWordOfCommand(String s) {
        if (isEmptyStrictly(s)) {
            return null;
        }
        if (s.contains("-") && s.contains("_")) {
            throw new IllegalArgumentException("[" + s + "]是一个格式错误指令，不能既包括'-'又包括'_'，只能使用其中一种");
        }
        if (s.contains("--") || s.contains("__")) {
            throw new IllegalArgumentException("[" + s + "]是一个格式错误指令，不能既包括'--'或包括'__'，横线单独出现用于分割词");
        }
        if (s.startsWith("-") || s.startsWith("_") || s.endsWith("-") || s.endsWith("_")) {
            throw new IllegalArgumentException("[" + s + "]是一个格式错误指令，不能已横线开始或结束");
        }
        String[] ss = {null, null};
        int i = 0;
        char c = s.charAt(i++);

        if (s.length() == 1) {
            if (isLowerCaseChar(c) || isUpperCaseChar(c)) {
                ss[0] = s;
                return ss;
            } else {
                throw new IllegalArgumentException("[" + s + "]是一个格式错误指令，指令应以英文字母开始");
            }
        }

        if (isLowerCaseChar(c)) {
            // 以小写字母开始
            for (; i < s.length(); i++) {
                c = s.charAt(i);
                if (isLowerCaseChar(c) || isNumberChar(c)) {
                    continue;
                } else if (isUpperCaseChar(c) || isCommandSplitChar(c)) {
                    // 以大写字母或横线结束
                    break;
                } else {
                    throw new IllegalArgumentException("[" + s + "]是一个格式错误指令，第 " + (i + 1) + " 个字符非指令字符");
                }
            }
        } else if (isUpperCaseChar(c)) {
            // 以大写字母开始
            char pc = s.charAt(i++);
            if (isUpperCaseChar(pc) || isNumberChar(pc)) {
                // 大写跟大写或数字
                for (; i < s.length(); i++) {
                    c = s.charAt(i);
                    if (isUpperCaseChar(c) || isNumberChar(c)) {
                        pc = c;
                        continue;
                    } else if (isLowerCaseChar(c)) {
                        if (isUpperCaseChar(pc)) {
                            i--; //小写字符结束，并且前一位是大写字符
                        }
                        break;
                    } else if (isCommandSplitChar(c)) {
                        break;
                    } else {
                        throw new IllegalArgumentException("[" + s + "]是一个格式错误指令，第 " + (i + 1) + " 个字符非指令字符");
                    }
                }
//                return s.substring(0, i);
            } else if (isLowerCaseChar(pc)) {
                // 大写跟小写
                for (; i < s.length(); i++) {
                    c = s.charAt(i);
                    if (isLowerCaseChar(c) || isNumberChar(c)) {
                        continue;
                    } else if (isUpperCaseChar(c) || isCommandSplitChar(c)) {
                        break;
                    } else {
                        throw new IllegalArgumentException("[" + s + "]是一个格式错误指令，第 " + (i + 1) + " 个字符非指令字符");
                    }
                }
//                return s.substring(0, i);
            } else if (isCommandSplitChar(pc)) {
                // 大写跟横线
//                return s.substring(0, i);
            } else {
                throw new IllegalArgumentException("[" + s + "]是一个格式错误指令，第 " + (i + 1) + " 个字符非指令字符");
            }
        } else {
            throw new IllegalArgumentException("[" + s + "]是一个格式错误指令，第 " + (i + 1) + " 个字符非指令字符");
        }
        ss[0] = s.substring(0, i);
        if (i < s.length()) {
            if (isCommandSplitChar(c)) {
                ss[1] = s.substring(i + 1);
            } else {
                ss[1] = s.substring(i);
            }
        }
        return ss;
    }

    public static String toFormatedCommand(String cmd) {
        if (cmd == null) {
            return null;
        }
        String formated = "";
        String[] ss = firstWordOfCommand(cmd);
        if (ss[1] != null) {
            formated = formated + ss[0].toLowerCase() + "-" + toFormatedCommand(ss[1]);
        } else {
            formated = formated + ss[0].toLowerCase();
        }
        return formated;
    }

    //如果不能满足要求，请留言并说明情况
    public static void main(String[] args) {
//        System.err.println(firstWordOfCommand("IThisIsACommand")[1]);
//        System.err.println(firstWordOfCommand("ACommand")[1]);
//        System.err.println(firstWordOfCommand("Nu009Ll-Sasdfa-asdfjkj-print")[1]);
//        System.err.println(firstWordOfCommand("null")[1]);
//        System.err.println(firstWordOfCommand("nulla0Sprint")[1]);
        
        
        Object a = new String[]{"a", "b"};
        if (a.getClass().isArray()) {
            D.trace("true");
        }
        if (a instanceof List) {
            D.trace("true");
        }
        
        List b = new ArrayList();
        if (b instanceof List) {
            D.trace("true");
        } else {
            D.trace("false");
        }
    }

}
