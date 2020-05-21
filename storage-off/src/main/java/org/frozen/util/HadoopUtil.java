package org.frozen.util;

import com.google.common.collect.Lists;
import net.sf.json.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

public class HadoopUtil {

    private static final Logger log = LoggerFactory.getLogger(HadoopUtil.class);

    public static final Charset UTF8 = Charset.forName("UTF-8");

    public static Job getJobInstance(Configuration configuration) throws IOException {
        return Job.getInstance(configuration);
    }

    public static String getJobState(Job job) throws InterruptedException, IOException {
        return job.getStatus().getState().toString().toString();
    }

    /**
     * HDFS文件合并
     * @param srcFS
     * @param srcDir
     * @param dstFS
     * @param dstFile
     * @param deleteSource
     * @param conf
     * @return
     * @throws IOException
     */
    public static boolean copyMerge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, boolean deleteSource, Configuration conf) throws IOException {
//        dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);

        if (!srcFS.getFileStatus(srcDir).isDirectory())
            return false;

        OutputStream out = dstFS.create(dstFile);

        try {
            FileStatus contents[] = srcFS.listStatus(srcDir);
            Arrays.sort(contents);
            for (int i = 0; i < contents.length; i++) {
                if (contents[i].isFile()) {
                    InputStream in = srcFS.open(contents[i].getPath());
                    try {
                        org.apache.hadoop.io.IOUtils.copyBytes(in, out, conf, false);
                    } finally {
                        in.close();
                    }
                }
            }
        } finally {
            out.close();
        }


        if (deleteSource) {
            return srcFS.delete(srcDir, true);
        } else {
            return true;
        }
    }

    /**
     * 按行读取文件内容.
     *
     * @param path
     * @return
     * @throws IOException
     */
    public static List<String> readTextFromFile(FileSystem fs, Path path) throws IOException {
        if (!fs.exists(path)) {
            return Lists.newArrayList();
        }
        if (fs.isDirectory(path)) {
            throw new IOException("Path is not File: " + path);
        }
        InputStream input = fs.open(path);
        List<String> lines = IOUtils.readLines(input, UTF8);
        IOUtils.closeQuietly(input);
        return lines;
    }


    /**
     * 返回目录下包含文件的所有子目录
     *
     * @param fs
     * @param path
     * @param pathList
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static Set<String> containFileDir(FileSystem fs, Path path, Set<String> pathList) throws FileNotFoundException, IOException {

        FileStatus[] fileStatuses = fs.listStatus(path);

        for (FileStatus fileStatus : fileStatuses) {

            Path filePath = fileStatus.getPath();
            if (fileStatus.isDirectory()) {
                containFileDir(fs, filePath, pathList);
            } else {
                pathList.add(filePath.getParent().toString());
            }
        }
        return pathList;
    }
    
    /**
     * 返回指定目录下的所有一级子目录
     * 
     * @param fs
     * @param path
     * @return
     * @throws Exception
     */
    public static Set<Path> dirSubdirectory(FileSystem fs, Path path) throws Exception {
    	FileStatus[] fileStatuses = fs.listStatus(path);
    	Set<Path> pathList = new HashSet<Path>();

        for (FileStatus fileStatus : fileStatuses) {
            pathList.add(fileStatus.getPath());
        }
        return pathList;
    }

    /**
     * 返回目录下包含_SUCCESS文件的所有目录
     *
     * @param fs
     * @param path
     * @param pathList
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static Set<String> containSUCCESSDir(FileSystem fs, Path path, Set<String> pathList) throws FileNotFoundException, IOException {

        FileStatus[] fileStatuses = fs.listStatus(path);

        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                String dirPath = fileStatus.getPath().toString();
                String sucFile = dirPath + "/_SUCCESS";
                if(fileExists(fs, new Path(sucFile))) {
                    pathList.add(dirPath);
                }
            }
        }
        return pathList;
    }

    /**
     * 返回目录下所有子文件
     *
     * @param fs
     * @param path
     * @param pathList
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static Set<String> containFilePath(FileSystem fs, Path path, Set<String> pathList) throws FileNotFoundException, IOException {

        FileStatus[] fileStatuses = fs.listStatus(path);

        for (FileStatus fileStatus : fileStatuses) {

            Path filePath = fileStatus.getPath();
            if (fileStatus.isDirectory()) {
                containFilePath(fs, filePath, pathList);
            } else {
                if(!filePath.getName().startsWith("_")) {
                    pathList.add(filePath.toString());
                }
            }
        }
        return pathList;
    }

    /**
     * 判断文件是否存在
     *
     * @param fs
     * @param path
     * @return
     * @throws IOException
     */
    public static boolean fileExists(FileSystem fs, Path path) throws IOException {
        return fs.exists(path);
    }

    /**
     * 创建目录
     */
    public static boolean mkdir(FileSystem fileSystem, Path path) throws Exception {
        return fileSystem.mkdirs(path);
    }

    /**
     * 删除
     */
    public static boolean delete(FileSystem fileSystem, Path path, boolean isRecursion) throws Exception {
        return fileSystem.delete(path, isRecursion);//true， 递归删除
    }

    /**
     * 递归删除文件夹，如果存在文件则移动到目录文件夹
     */
    public static void deleteR(FileSystem fs, Path originalPath, Path targetPath) throws Exception {
        if(!fs.getFileStatus(originalPath).isDirectory()) { // 如果是文件
            HadoopUtil.mv(fs, originalPath, targetPath);
        } else { // 如果是文件夹
            FileStatus[] fileStatuses = fs.listStatus(originalPath);

            for (FileStatus fileStatus : fileStatuses) {
                Path filePath = fileStatus.getPath();
                if (fileStatus.isDirectory()) {

                    if(fs.listStatus(filePath).length > 0) {
                        deleteR(fs, filePath, targetPath);
                        HadoopUtil.delete(fs, filePath, false);
                    } else {
                        HadoopUtil.delete(fs, filePath, false);
                    }
                } else {
                    if(!filePath.getName().startsWith("_")) {
                        HadoopUtil.mv(fs, filePath, targetPath);
                    } else {
                        HadoopUtil.delete(fs, filePath, false);
                    }
                }
            }
        }
    }

    /**
     * 移动数据
     */
    public static boolean mv(FileSystem fileSystem, Path oldPath, Path newPath) throws Exception {
        return fileSystem.rename(oldPath, newPath);
    }

    /**
     * 文件重命名
     */
    public static boolean rename(FileSystem fileSystem, Path fileOldName, Path fileNewName) throws Exception {
        return fileSystem.rename(fileOldName, fileNewName);
    }

    /**
     * 移动目录下的文件(夹)到目标目录，文件夹名相同则合并，文件名相同则文件重命名
     */
    public static void mvAlsoMerge(FileSystem fileSystem, Set<Path> sourcePathSet, Path targetPath, Boolean isDelSource) throws Exception {
        for(Path rootPath : sourcePathSet) {
            FileStatus[] listFile = fileSystem.listStatus(rootPath);
            for(FileStatus fileStatus : listFile) {
                Path originalPathChild = fileStatus.getPath();
                Path targetPathChild = new Path(targetPath + "/" + originalPathChild.getName());

                if (fileStatus.isDirectory()) {
                    if (fileExists(fileSystem, targetPathChild)) {

                        Set<Path> sucPathSet = new HashSet<Path>(); // 返回所有子目录
                        sucPathSet.add(originalPathChild);

                        mvAlsoMerge(fileSystem, sucPathSet, targetPathChild, isDelSource);
                    } else {
                        HadoopUtil.mv(fileSystem, originalPathChild, targetPath);
                    }
                } else {
                    if (originalPathChild.getName().startsWith("_SUCCESS")) {
                        delete(fileSystem, originalPathChild, false);
                    } else {
                        if (fileExists(fileSystem, targetPathChild)) {
                            HadoopUtil.mv(fileSystem, originalPathChild, new Path(targetPath + "/" + originalPathChild.getName() + "_" + UUID.randomUUID().toString()));
                        } else {
                            HadoopUtil.mv(fileSystem, originalPathChild, new Path(targetPath + "/" + originalPathChild.getName()));
                        }
                    }
                }

                FileStatus[] endingListFile = fileSystem.listStatus(rootPath);
                if (endingListFile.length == 0 && isDelSource) { // 如果此目录下所有文件已经移动完成，则删除目录
                    delete(fileSystem, rootPath, false);
                }
            }
        }
    }

    /**
     * 返回目录下的所有子目录
     */
    public static Set<String> childDir(FileSystem fileSystem, Path path) throws Exception {
        Set<String> pathSet = new HashSet<String>();

        FileStatus[] listFile = fileSystem.listStatus(path);

        for(FileStatus fileStatus : listFile) {
            pathSet.add(fileStatus.getPath().toString());
        }

        return pathSet;
    }

    /**
     * 目录下所有文件(除_SUCCESS文件)(包括子文件夹中的文件)重命名，并返回带有_SUCCESS文件的父目录
     */
    public static void batchRenameFile(FileSystem fileSystem, Path originalPath) throws Exception {
        Set<String> pathSet = new HashSet<String>();
        HadoopUtil.containFilePath(fileSystem, originalPath, pathSet);

        for(String path : pathSet) { // 所有文件重命名
            HadoopUtil.rename(fileSystem, new Path(path), new Path(path + "_" + UUID.randomUUID().toString()));
        }
    }


    /**
     * 将list数据逐行写入HDFS文本文件
     *
     * @param fileSystem
     * @param path
     * @param lines
     * @param overwrite 是否覆盖源文件
     * @throws IOException
     */
    public static void writeTextToFile(FileSystem fileSystem, Path path, List<String> lines, boolean overwrite) throws IOException {
        OutputStream output = fileSystem.create(path, overwrite);
        IOUtils.writeLines(lines, "\n", output, UTF8);
        IOUtils.closeQuietly(output);
    }

    /**
     * 将List数据逐行追加到HDFS文本文件
     * @param fileSystem
     * @param path
     * @param lines
     * @throws Exception
     */
    public static void appendTextToFile(FileSystem fileSystem, Path path, List<String> lines) throws Exception {
        if(fileExists(fileSystem, path)) {
            lines.addAll(readTextFromFile(fileSystem, path));
        }
        writeTextToFile(fileSystem, path, lines, true);
    }

    /**
     * 拷贝文件
     * @param src
     * @param dst
     * @param fileSystem
     * @return
     * @throws Exception
     */
    public static boolean copyFile(Path src, Path dst, FileSystem fileSystem) throws Exception {
        fileSystem.exists(dst);
        File file = new File(src.toString());

        InputStream in = new BufferedInputStream(new FileInputStream(file));

        // FieSystem的create方法可以为文件不存在的父目录进行创建
        OutputStream out = fileSystem.create(dst);
        org.apache.hadoop.io.IOUtils.copyBytes(in, out, 4096, true);
        return true;
    }

    /**
     * 拷贝文件夹
     * @param src
     * @param dst
     * @param fileSystem
     * @return
     * @throws Exception
     */
    public static boolean copyDirectory(Path src, Path dst, FileSystem fileSystem) throws Exception {
        if(!fileSystem.exists(dst)){
            mkdir(fileSystem, dst);
        }

        FileStatus status = fileSystem.getFileStatus(dst);
        File file = new File(src.toString());

        if(status.isFile()){
            System.exit(2);
        }

        File[] files = file.listFiles();
        for(int i = 0 ;i< files.length; i ++){
            File f = files[i];
            if(f.isDirectory()){
                copyDirectory(new Path(f.getPath()), dst, fileSystem);
            }else{
                copyFile(new Path(f.getPath()), new Path(dst.toString() + files[i].getName()), fileSystem);
            }
        }
        return true;
    }
}
