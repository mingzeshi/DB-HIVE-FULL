package com.jy.util;

import java.net.URLDecoder;
import java.net.URLEncoder;

public class CoderUtil {

    /**
     * 解码
     * @param str
     * @return
     * @throws Exception
     */
    public static String decoder(String str) throws Exception {
//        return URLDecoder.decode(str, "GBK");
        return URLDecoder.decode(str, "UTF-8");
    }

    /**
     * 编码
     * @param str
     * @return
     * @throws Exception
     */
    public static String encoder(String str) throws  Exception {
//        return URLEncoder.encode(str, "GBK");
        return URLEncoder.encode(str, "utf-8");
    }
}
