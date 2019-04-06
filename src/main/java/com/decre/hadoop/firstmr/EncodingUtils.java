package com.decre.hadoop.firstmr;

import org.apache.hadoop.io.Text;

import java.io.UnsupportedEncodingException;

/**
 * @author Decre
 * @date 2019/4/6 0006 10:42
 * @since 1.0.0
 * Descirption:
 */
public class EncodingUtils {

    /**
     * 将hadoop中Text内容编码转换成指定编码格式，因为hadoop里面写死了utf-8，导致中文会乱码
     * @param text
     * @param encoding
     * @return
     */
    public static Text transformTextToUTF8(Text text, String encoding) {
        String value = null;
        try {
            value = new String(text.getBytes(), 0, text.getLength(), encoding);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new Text(value);
    }
}
