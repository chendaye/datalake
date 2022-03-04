package top.chendaye666.reflect;

import lombok.extern.slf4j.Slf4j;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * 使用反射创建对象
 */
@Slf4j
public class  reflectUtils {
    public static Object  getObj(String className, String logData){
        try {
            Class<Object> clazz = (Class<Object>) Class.forName(className);
            //有参构造方法
            Constructor<Object> cons = clazz.getConstructor(new Class[]{String.class});
            Object obj = cons.newInstance(new Object[] {logData});
            return obj;
        } catch (Exception e) {
            log.info("类："+className+"创建失败！");
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 反射获取 正则匹解析日志的类
     * @param className
     * @param logData
     * @param reg
     * @param regIndex
     * @return
     */
    public static Object  getIndexRegObj(String className, String logData, String reg, int regIndex){
        try {
            Class<Object> clazz = (Class<Object>) Class.forName(className);
            //有参构造方法
            Constructor<Object> cons = clazz.getConstructor(new Class[]{String.class, String.class, int.class});
            Object obj = cons.newInstance(new Object[] {logData, reg, regIndex});
            return obj;
        } catch (Exception e) {
            log.info("类："+className+"创建失败！");
            e.printStackTrace();
            return null;
        }
    }


    /**
     * 反射调用对象实例的 getter方法
     * @param obj
     * @param methodName
     * @return
     */
    public static Object invokeGet(Object obj, String methodName){
        try {
            Class<?> cls = obj.getClass();
            String getMethodName="get"+methodName;
            Method method = cls.getDeclaredMethod(getMethodName);   //getter没有参数
            return method.invoke(obj);
        }catch (Exception e){
            log.info("调用类："+obj.getClass()+"的方法："+methodName+"失败！");
            e.printStackTrace();
            return null;
        }
    }

    public static Object invokeMethod(Object obj, String methodName){
        try {
            Class<?> cls = obj.getClass();
            Method method = cls.getDeclaredMethod(methodName);   //getter没有参数
            return method.invoke(obj);
        }catch (Exception e){
            log.info("调用类："+obj.getClass()+"的方法："+methodName+"失败！");
            e.printStackTrace();
            return null;
        }
    }
}
