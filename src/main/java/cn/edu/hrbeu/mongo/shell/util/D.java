/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hrbeu.mongo.shell.util;


/**
 *
 * @author wuxiang
 */
public class D {
    public static void trace(Object ...msgs) {
        StringBuilder sb = new StringBuilder();
        for (Object s : msgs) {
            sb.append(s);
        }
        System.err.println(">" + sb.toString());
    }

    public static void wait(int millis) {
        sleep(millis);
    }

    public static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
        }
    }

    public static String pact(Object... msgs) {
        StringBuilder sb = new StringBuilder();
        for (Object s : msgs) {
            sb.append(s);
        }
        return sb.toString();
    }    
}
