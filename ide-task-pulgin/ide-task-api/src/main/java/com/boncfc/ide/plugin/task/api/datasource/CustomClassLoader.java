package com.boncfc.ide.plugin.task.api.datasource;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CustomClassLoader extends ClassLoader{
    private String path;
    public CustomClassLoader(String path) {
        this.path = path;
    }
    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        try {
            byte[] data = loadClassData(name);
            return defineClass(name, data, 0, data.length);
        } catch (IOException e) {
            throw new ClassNotFoundException();
        }
    }
    private byte[] loadClassData(String className) throws IOException {
        // 根据类名和路径加载字节码数据
        // 这里需要根据实际情况构建正确的文件路径来读取字节码文件
        // 例如，如果是".class"文件格式，可以按照包名构建路径
        String file = path + className.replace('.', '/') + ".class";
        InputStream in = Files.newInputStream(Paths.get(file));
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int data = in.read();
        while (data!= -1) {
            buffer.write(data);
            data = in.read();
        }
        in.close();
        return buffer.toByteArray();
    }
}
