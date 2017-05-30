/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hrbeu.mongo.shell.util;

import java.text.NumberFormat;

/**
 *
 * @author Administrator
 */
public class Numbers {

    // 15.4表示最大长度15，精度4，其余位用空格补齐
    public static String toFormatedString(Double x, int fixDigits, int fractionDigits) {
        NumberFormat ddf1 = NumberFormat.getNumberInstance();
        ddf1.setMaximumFractionDigits(fractionDigits);
        ddf1.setMinimumFractionDigits(fractionDigits);
        String s = ddf1.format(x);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fixDigits - s.length(); i++) {
            sb.append(" ");
        }
        sb.append(s);
        return sb.toString();
    }

    public static void main(String args[]) {
        double x = 3.1415926;
        System.out.print(toFormatedString(x, 15, 4));
    }
}
