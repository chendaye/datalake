package top.chendaye666.utils;

public class StringUtils {
    /**
     * 首字母大写
     * @param str
     * @return
     */
    public static String upperCaseFirst(String str){
        char[] ch = str.toCharArray();
        if (ch[0] >= 'a' && ch[0] <= 'z')
            ch[0] = (char)(ch[0] - 32);
        return new String(ch);
    }

    public static String upperCaseFirst2(String str){
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}
