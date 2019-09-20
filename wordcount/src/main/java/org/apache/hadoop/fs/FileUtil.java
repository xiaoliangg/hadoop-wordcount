package org.apache.hadoop.fs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;

public class FileUtil
{
  private static final Log LOG = LogFactory.getLog(FileUtil.class);
  
  public static Path[] stat2Paths(FileStatus[] stats)
  {
    if (stats == null) {
      return null;
    }
    Path[] ret = new Path[stats.length];
    for (int i = 0; i < stats.length; i++) {
      ret[i] = stats[i].getPath();
    }
    return ret;
  }
  
  public static Path[] stat2Paths(FileStatus[] stats, Path path)
  {
    if (stats == null) {
      return new Path[] { path };
    }
    return stat2Paths(stats);
  }
  
  public static boolean fullyDelete(File dir)
    throws IOException
  {
    if (!fullyDeleteContents(dir)) {
      return false;
    }
    return dir.delete();
  }
  
  public static boolean fullyDeleteContents(File dir)
    throws IOException
  {
    boolean deletionSucceeded = true;
    File[] contents = dir.listFiles();
    if (contents != null) {
      for (int i = 0; i < contents.length; i++) {
        if (contents[i].isFile())
        {
          if (!contents[i].delete()) {
            deletionSucceeded = false;
          }
        }
        else
        {
          boolean b = false;
          b = contents[i].delete();
          if (!b) {
            if (!fullyDelete(contents[i])) {
              deletionSucceeded = false;
            }
          }
        }
      }
    }
    return deletionSucceeded;
  }
  
  @Deprecated
  public static void fullyDelete(FileSystem fs, Path dir)
    throws IOException
  {
    fs.delete(dir, true);
  }
  
  private static void checkDependencies(FileSystem srcFS, Path src, FileSystem dstFS, Path dst)
    throws IOException
  {
    if (srcFS == dstFS)
    {
      String srcq = src.makeQualified(srcFS).toString() + "/";
      String dstq = dst.makeQualified(dstFS).toString() + "/";
      if (dstq.startsWith(srcq))
      {
        if (srcq.length() == dstq.length()) {
          throw new IOException("Cannot copy " + src + " to itself.");
        }
        throw new IOException("Cannot copy " + src + " to its subdirectory " + dst);
      }
    }
  }
  
  public static boolean copy(FileSystem srcFS, Path src, FileSystem dstFS, Path dst, boolean deleteSource, Configuration conf)
    throws IOException
  {
    return copy(srcFS, src, dstFS, dst, deleteSource, true, conf);
  }
  
  public static boolean copy(FileSystem srcFS, Path[] srcs, FileSystem dstFS, Path dst, boolean deleteSource, boolean overwrite, Configuration conf)
    throws IOException
  {
    boolean gotException = false;
    boolean returnVal = true;
    StringBuffer exceptions = new StringBuffer();
    if (srcs.length == 1) {
      return copy(srcFS, srcs[0], dstFS, dst, deleteSource, overwrite, conf);
    }
    if (!dstFS.exists(dst)) {
      throw new IOException("`" + dst + "': specified destination directory " + "does not exist");
    }
    FileStatus sdst = dstFS.getFileStatus(dst);
    if (!sdst.isDir()) {
      throw new IOException("copying multiple files, but last argument `" + dst + "' is not a directory");
    }
    for (Path src : srcs) {
      try
      {
        if (!copy(srcFS, src, dstFS, dst, deleteSource, overwrite, conf)) {
          returnVal = false;
        }
      }
      catch (IOException e)
      {
        gotException = true;
        exceptions.append(e.getMessage());
        exceptions.append("\n");
      }
    }
    if (gotException) {
      throw new IOException(exceptions.toString());
    }
    return returnVal;
  }
  
  public static boolean copy(FileSystem srcFS, Path src, FileSystem dstFS, Path dst, boolean deleteSource, boolean overwrite, Configuration conf)
    throws IOException
  {
    dst = checkDest(src.getName(), dstFS, dst, overwrite);
    if (srcFS.getFileStatus(src).isDir())
    {
      checkDependencies(srcFS, src, dstFS, dst);
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      FileStatus[] contents = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i].getPath(), dstFS, new Path(dst, contents[i].getPath().getName()), deleteSource, overwrite, conf);
      }
    }
    else if (srcFS.isFile(src))
    {
      InputStream in = null;
      OutputStream out = null;
      try
      {
        in = srcFS.open(src);
        out = dstFS.create(dst, overwrite);
        IOUtils.copyBytes(in, out, conf, true);
      }
      catch (IOException e)
      {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        throw e;
      }
    }
    else
    {
      throw new IOException(src.toString() + ": No such file or directory");
    }
    if (deleteSource) {
      return srcFS.delete(src, true);
    }
    return true;
  }
  
  public static boolean copyMerge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, boolean deleteSource, Configuration conf, String addString)
    throws IOException
  {
    dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);
    if (!srcFS.getFileStatus(srcDir).isDir()) {
      return false;
    }
    OutputStream out = dstFS.create(dstFile);
    try
    {
      FileStatus[] contents = srcFS.listStatus(srcDir);
      for (int i = 0; i < contents.length; i++) {
        if (!contents[i].isDir())
        {
          InputStream in = srcFS.open(contents[i].getPath());
          try
          {
            IOUtils.copyBytes(in, out, conf, false);
            if (addString != null) {
              out.write(addString.getBytes("UTF-8"));
            }
          }
          finally {}
        }
      }
    }
    finally
    {
      out.close();
    }
    if (deleteSource) {
      return srcFS.delete(srcDir, true);
    }
    return true;
  }
  
  public static boolean copy(File src, FileSystem dstFS, Path dst, boolean deleteSource, Configuration conf)
    throws IOException
  {
    dst = checkDest(src.getName(), dstFS, dst, false);
    if (src.isDirectory())
    {
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      File[] contents = listFiles(src);
      for (int i = 0; i < contents.length; i++) {
        copy(contents[i], dstFS, new Path(dst, contents[i].getName()), deleteSource, conf);
      }
    }
    else if (src.isFile())
    {
      InputStream in = null;
      OutputStream out = null;
      try
      {
        in = new FileInputStream(src);
        out = dstFS.create(dst);
        IOUtils.copyBytes(in, out, conf);
      }
      catch (IOException e)
      {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        throw e;
      }
    }
    else
    {
      throw new IOException(src.toString() + ": No such file or directory");
    }
    if (deleteSource) {
      return fullyDelete(src);
    }
    return true;
  }
  
  public static boolean copy(FileSystem srcFS, Path src, File dst, boolean deleteSource, Configuration conf)
    throws IOException
  {
    if (srcFS.getFileStatus(src).isDir())
    {
      if (!dst.mkdirs()) {
        return false;
      }
      FileStatus[] contents = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i].getPath(), new File(dst, contents[i].getPath().getName()), deleteSource, conf);
      }
    }
    else if (srcFS.isFile(src))
    {
      InputStream in = srcFS.open(src);
      IOUtils.copyBytes(in, new FileOutputStream(dst), conf);
    }
    else
    {
      throw new IOException(src.toString() + ": No such file or directory");
    }
    if (deleteSource) {
      return srcFS.delete(src, true);
    }
    return true;
  }
  
  private static Path checkDest(String srcName, FileSystem dstFS, Path dst, boolean overwrite)
    throws IOException
  {
    if (dstFS.exists(dst))
    {
      FileStatus sdst = dstFS.getFileStatus(dst);
      if (sdst.isDir())
      {
        if (null == srcName) {
          throw new IOException("Target " + dst + " is a directory");
        }
        return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
      }
      if (!overwrite) {
        throw new IOException("Target " + dst + " already exists");
      }
    }
    else if (dst.toString().isEmpty())
    {
      return checkDest(null, dstFS, new Path(srcName), overwrite);
    }
    return dst;
  }
  
  private static class CygPathCommand
    extends Shell
  {
    String[] command;
    String result;
    
    CygPathCommand(String path)
      throws IOException
    {
      this.command = new String[] { "cygpath", "-u", path };
      run();
    }
    
    String getResult()
      throws IOException
    {
      return this.result;
    }
    
    protected String[] getExecString()
    {
      return this.command;
    }
    
    protected void parseExecResult(BufferedReader lines)
      throws IOException
    {
      String line = lines.readLine();
      if (line == null) {
        throw new IOException("Can't convert '" + this.command[2] + " to a cygwin path");
      }
      this.result = line;
    }
  }
  
  public static String makeShellPath(String filename)
    throws IOException
  {
    if (Path.WINDOWS) {
      return new CygPathCommand(filename).getResult();
    }
    return filename;
  }
  
  public static String makeShellPath(File file)
    throws IOException
  {
    return makeShellPath(file, false);
  }
  
  public static String makeShellPath(File file, boolean makeCanonicalPath)
    throws IOException
  {
    if (makeCanonicalPath) {
      return makeShellPath(file.getCanonicalPath());
    }
    return makeShellPath(file.toString());
  }
  
  public static long getDU(File dir)
  {
    long size = 0L;
    if (!dir.exists()) {
      return 0L;
    }
    if (!dir.isDirectory()) {
      return dir.length();
    }
    File[] allFiles = dir.listFiles();
    if (allFiles != null) {
      for (int i = 0; i < allFiles.length; i++)
      {
        boolean isSymLink;
        try
        {
          isSymLink = FileUtils.isSymlink(allFiles[i]);
        }
        catch (IOException ioe)
        {
          isSymLink = true;
        }
        if (!isSymLink) {
          size += getDU(allFiles[i]);
        }
      }
    }
    return size;
  }
  
  public static void unZip(File inFile, File unzipDir)
    throws IOException
  {
    ZipFile zipFile = new ZipFile(inFile);
    try
    {
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements())
      {
        ZipEntry entry = (ZipEntry)entries.nextElement();
        if (!entry.isDirectory())
        {
          InputStream in = zipFile.getInputStream(entry);
          try
          {
            File file = new File(unzipDir, entry.getName());
            if ((!file.getParentFile().mkdirs()) && 
              (!file.getParentFile().isDirectory())) {
              throw new IOException("Mkdirs failed to create " + file.getParentFile().toString());
            }
            OutputStream out = new FileOutputStream(file);
            try
            {
              byte[] buffer = new byte['?'];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            }
            finally {}
          }
          finally {}
        }
      }
    }
    finally
    {
      zipFile.close();
    }
  }
  
  public static void unTar(File inFile, File untarDir)
    throws IOException
  {
    if ((!untarDir.mkdirs()) && 
      (!untarDir.isDirectory())) {
      throw new IOException("Mkdirs failed to create " + untarDir);
    }
    StringBuffer untarCommand = new StringBuffer();
    boolean gzipped = inFile.toString().endsWith("gz");
    if (gzipped)
    {
      untarCommand.append(" gzip -dc '");
      untarCommand.append(makeShellPath(inFile));
      untarCommand.append("' | (");
    }
    untarCommand.append("cd '");
    untarCommand.append(makeShellPath(untarDir));
    untarCommand.append("' ; ");
    untarCommand.append("tar -xf ");
    if (gzipped) {
      untarCommand.append(" -)");
    } else {
      untarCommand.append(makeShellPath(inFile));
    }
    String[] shellCmd = { "bash", "-c", untarCommand.toString() };
    Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(shellCmd);
    shexec.execute();
    int exitcode = shexec.getExitCode();
    if (exitcode != 0) {
      throw new IOException("Error untarring file " + inFile + ". Tar process exited with exit code " + exitcode);
    }
  }
  
  public static int symLink(String target, String linkname)
    throws IOException
  {
    String cmd = "ln -s " + target + " " + linkname;
    Process p = Runtime.getRuntime().exec(cmd, null);
    int returnVal = -1;
    try
    {
      returnVal = p.waitFor();
    }
    catch (InterruptedException e) {}
    if (returnVal != 0) {
      LOG.warn("Command '" + cmd + "' failed " + returnVal + " with: " + copyStderr(p));
    }
    return returnVal;
  }
  
  private static String copyStderr(Process p)
    throws IOException
  {
    InputStream err = p.getErrorStream();
    StringBuilder result = new StringBuilder();
    byte[] buff = new byte['?'];
    int len = err.read(buff);
    while (len > 0)
    {
      result.append(new String(buff, 0, len));
      len = err.read(buff);
    }
    return result.toString();
  }
  
  public static int chmod(String filename, String perm)
    throws IOException, InterruptedException
  {
    return chmod(filename, perm, false);
  }
  
  public static int chmod(String filename, String perm, boolean recursive)
    throws IOException
  {
    StringBuffer cmdBuf = new StringBuffer();
    cmdBuf.append("chmod ");
    if (recursive) {
      cmdBuf.append("-R ");
    }
    cmdBuf.append(perm).append(" ");
    cmdBuf.append(filename);
    String[] shellCmd = { "bash", "-c", cmdBuf.toString() };
    Shell.ShellCommandExecutor shExec = new Shell.ShellCommandExecutor(shellCmd);
    try
    {
      shExec.execute();
    }
    catch (IOException e)
    {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Error while changing permission : " + filename + " Exception: " + StringUtils.stringifyException(e));
      }
    }
    return shExec.getExitCode();
  }
  
  public static void setPermission(File f, FsPermission permission)
    throws IOException
  {
    FsAction user = permission.getUserAction();
    FsAction group = permission.getGroupAction();
    FsAction other = permission.getOtherAction();
    if ((group != other) || (NativeIO.isAvailable()))
    {
      execSetPermission(f, permission);
      return;
    }
    boolean rv = true;
    
    rv = f.setReadable(group.implies(FsAction.READ), false);
    checkReturnValue(rv, f, permission);
    if (group.implies(FsAction.READ) != user.implies(FsAction.READ))
    {
      f.setReadable(user.implies(FsAction.READ), true);
      checkReturnValue(rv, f, permission);
    }
    rv = f.setWritable(group.implies(FsAction.WRITE), false);
    checkReturnValue(rv, f, permission);
    if (group.implies(FsAction.WRITE) != user.implies(FsAction.WRITE))
    {
      f.setWritable(user.implies(FsAction.WRITE), true);
      checkReturnValue(rv, f, permission);
    }
    rv = f.setExecutable(group.implies(FsAction.EXECUTE), false);
    checkReturnValue(rv, f, permission);
    if (group.implies(FsAction.EXECUTE) != user.implies(FsAction.EXECUTE))
    {
      f.setExecutable(user.implies(FsAction.EXECUTE), true);
      checkReturnValue(rv, f, permission);
    }
  }
  
  private static void checkReturnValue(boolean rv, File p, FsPermission permission)
    throws IOException
  {
//    if (!rv) {
//      throw new IOException("Failed to set permissions of path: " + p + " to " + String.format("%04o", new Object[] { Short.valueOf(permission.toShort()) }));
//    }
  }
  
  private static void execSetPermission(File f, FsPermission permission)
    throws IOException
  {
    if (NativeIO.isAvailable()) {
      NativeIO.chmod(f.getCanonicalPath(), permission.toShort());
    } else {
      execCommand(f, new String[] { "chmod", String.format("%04o", new Object[] { Short.valueOf(permission.toShort()) }) });
    }
  }
  
  static String execCommand(File f, String... cmd)
    throws IOException
  {
    String[] args = new String[cmd.length + 1];
    System.arraycopy(cmd, 0, args, 0, cmd.length);
    args[cmd.length] = f.getCanonicalPath();
    String output = Shell.execCommand(args);
    return output;
  }
  
  public static final File createLocalTempFile(File basefile, String prefix, boolean isDeleteOnExit)
    throws IOException
  {
    File tmp = File.createTempFile(prefix + basefile.getName(), "", basefile.getParentFile());
    if (isDeleteOnExit) {
      tmp.deleteOnExit();
    }
    return tmp;
  }
  
  public static void replaceFile(File src, File target)
    throws IOException
  {
    if (!src.renameTo(target))
    {
      int retries = 5;
      while ((target.exists()) && (!target.delete()) && (retries-- >= 0)) {
        try
        {
          Thread.sleep(1000L);
        }
        catch (InterruptedException e)
        {
          throw new IOException("replaceFile interrupted.");
        }
      }
      if (!src.renameTo(target)) {
        throw new IOException("Unable to rename " + src + " to " + target);
      }
    }
  }
  
  public static File[] listFiles(File dir)
    throws IOException
  {
    File[] files = dir.listFiles();
    if (files == null) {
      throw new IOException("Invalid directory or I/O error occurred for dir: " + dir.toString());
    }
    return files;
  }
  
  public static String[] list(File dir)
    throws IOException
  {
    String[] fileNames = dir.list();
    if (fileNames == null) {
      throw new IOException("Invalid directory or I/O error occurred for dir: " + dir.toString());
    }
    return fileNames;
  }
}
