/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hrbeu.mongo.shell.util;

/**
 *
 * @author Administrator
 */
public class Runtimes {
    
    public static class RuntemsException extends RuntimeException {
        private RuntemsException(String s) {
            super(s);
        }
    }
    
    public static void throwIf(boolean o, Object... msgs) {
        if (o == false) {
            return;
        }
        throw newException(msgs);
    }

    public static void throwIfNull(Object o, Object... msgs) {
        if (o != null) {
            return;
        }
        throw newException(msgs);
    }
    
    public static RuntimeException newException(Object... msgs) {
        if (msgs == null || msgs.length == 0) {
            return new RuntemsException("未知错误");
        }
        if (msgs.length == 1) {
            return new RuntemsException(msgs[0].toString());
        } else {
            StringBuilder sb = new StringBuilder();
            for (Object s : msgs) {
                sb.append(s);
            }
            return new RuntemsException(sb.toString());
        }
    }
}
