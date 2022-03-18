package top.chendaye666.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class JsonParamUtils {
    public File file;
    public String content;
    public JSONObject jsonObject;

    public JsonParamUtils(String path) throws IOException {
        this.file = new File(path);
        content = FileUtils.readFileToString(file);
        jsonObject = JSONObject.parseObject(content);
    }

    public String getString(String key){
        return jsonObject.getString(key);
    }

    public JSONObject getJson(String key){
        return jsonObject.getJSONObject(key);
    }
}
