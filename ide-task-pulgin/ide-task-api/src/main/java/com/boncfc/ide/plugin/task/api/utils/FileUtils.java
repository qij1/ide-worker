/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.boncfc.ide.plugin.task.api.utils;

import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;


@UtilityClass
@Slf4j
public class FileUtils {

    private static final Set<PosixFilePermission> PERMISSION_755 = PosixFilePermissions.fromString("rwxr-xr-x");


    /**
     * write content to file ,if parent path not exists, it will do one's utmost to mkdir
     *
     * @param content content
     * @param filePath target file path
     * @return true if write success
     */
    public static boolean writeContent2File(String content, String filePath) {
        FileOutputStream fos = null;
        try {
            File distFile = new File(filePath);
            if (!distFile.getParentFile().exists() && !distFile.getParentFile().mkdirs()) {
                log.error("mkdir parent failed");
                return false;
            }
            fos = new FileOutputStream(filePath);
            IOUtils.write(content, fos, StandardCharsets.UTF_8);
            setFileTo755(distFile);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return false;
        } finally {
            IOUtils.closeQuietly(fos);
        }
        return true;
    }

    /**
     * Deletes a file. If file is a directory, delete it and all sub-directories.
     * <p>
     * The difference between File.delete() and this method are:
     * <ul>
     * <li>A directory to be deleted does not have to be empty.</li>
     * <li>You get exceptions when a file or directory cannot be deleted.
     *      (java.io.File methods returns a boolean)</li>
     * </ul>
     *
     * @param filename file name
     */
    public static void deleteFile(String filename) {
        org.apache.commons.io.FileUtils.deleteQuietly(new File(filename));
    }

    /**
     * Get Content
     *
     * @param inputStream input stream
     * @return string of input stream
     */
    public static String readFile2Str(InputStream inputStream) {

        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = inputStream.read(buffer)) != -1) {
                output.write(buffer, 0, length);
            }
            return output.toString(StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Check whether the given string type of path can be traversal or not, return true if path could
     * traversal, and return false if it is not.
     *
     * @param filename String type of filename
     * @return whether file path could be traversal or not
     */
    public static boolean directoryTraversal(String filename) {
        if (filename.contains(File.separator)) {
            return true;
        }
        File file = new File(filename);
        try {
            File canonical = file.getCanonicalFile();
            File absolute = file.getAbsoluteFile();
            return !canonical.equals(absolute);
        } catch (IOException e) {
            return true;
        }
    }

    /**
     * Calculate file checksum with CRC32 algorithm
     * @param pathName
     * @return checksum of file/dir
     */
    public static String getFileChecksum(String pathName) throws IOException {
        CRC32 crc32 = new CRC32();
        File file = new File(pathName);
        String crcString = "";
        if (file.isDirectory()) {
            // file system interface remains the same order
            String[] subPaths = file.list();
            StringBuilder concatenatedCRC = new StringBuilder();
            for (String subPath : subPaths) {
                concatenatedCRC.append(getFileChecksum(pathName + File.separator + subPath));
            }
            crcString = concatenatedCRC.toString();
        } else {
            try (
                    FileInputStream fileInputStream = new FileInputStream(pathName);
                    CheckedInputStream checkedInputStream = new CheckedInputStream(fileInputStream, crc32);) {
                while (checkedInputStream.read() != -1) {
                }
            } catch (IOException e) {
                throw new IOException("Calculate checksum error.");
            }
            crcString = Long.toHexString(crc32.getValue());
        }

        return crcString;
    }

    public static void createFileWith755(@NonNull Path path) throws IOException {
        if (SystemUtils.IS_OS_WINDOWS) {
            Files.createFile(path);
        } else {
            Files.createFile(path);
            Files.setPosixFilePermissions(path, PERMISSION_755);
        }
    }

    public static void createDirectoryWith755(@NonNull Path path) throws IOException {
        if (path.toFile().exists()) {
            return;
        }
        if (SystemUtils.IS_OS_WINDOWS) {
            Files.createDirectories(path);
        } else {
            Path parent = path.getParent();
            if (parent != null && !parent.toFile().exists()) {
                createDirectoryWith755(parent);
            }

            try {
                Files.createDirectory(path);
                Files.setPosixFilePermissions(path, PERMISSION_755);
            } catch (FileAlreadyExistsException fileAlreadyExistsException) {
                // Catch the FileAlreadyExistsException here to avoid create the same parent directory in parallel
                log.debug("The directory: {} already exists", path);
            }

        }
    }

    public static void setFileTo755(File file) throws IOException {
        if (SystemUtils.IS_OS_WINDOWS) {
            return;
        }
        if (file.isFile()) {
            Files.setPosixFilePermissions(file.toPath(), PERMISSION_755);
            return;
        }
        Files.setPosixFilePermissions(file.toPath(), PERMISSION_755);
        File[] files = file.listFiles();
        if (files != null) {
            for (File f : files) {
                setFileTo755(f);
            }
        }
    }

}
