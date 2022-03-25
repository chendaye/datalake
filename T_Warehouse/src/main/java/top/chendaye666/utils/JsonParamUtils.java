package top.chendaye666.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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

    /**
     * 获取table name 列表
     * @return
     */
    public HashSet<String> getTableSet(){
        HashSet<String> tableSet = new HashSet<>();
        JSONObject sourceType = getJson("sourceType");
        Set<Map.Entry<String, Object>> entries = sourceType.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            JSONObject value = (JSONObject)next.getValue();
            String name = value.getString("tableName");
            tableSet.add(name);
        }
        return tableSet;
    }
}
