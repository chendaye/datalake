package top.chendaye666.utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Props {
    public static Properties load(String path) throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(path)) ;
        Properties p = new Properties();
        p.load(in);
        return p;
    }
}
