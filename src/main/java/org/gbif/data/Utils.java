package org.gbif.data;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class Utils {

  public static void copy(String resource, File to) throws IOException {
    InputStream stream = Utils.class.getResourceAsStream(resource);
    Files.copy(stream, to.toPath());
  }

  public static void zip(Path dir, Path to, String... excludeFiles) throws IOException {
    final Set<String> exclusion = new HashSet<>();
    if (excludeFiles != null) {
      exclusion.addAll(Arrays.asList(excludeFiles));
    }
    Path p = Files.createFile(to);
    try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(p))) {
      Files.walk(dir)
          .filter(path -> !Files.isDirectory(path) && !exclusion.contains(path.getFileName().toString()))
          .forEach(path -> {
            ZipEntry zipEntry = new ZipEntry(dir.relativize(path).toString());
            try {
              zs.putNextEntry(zipEntry);
              Files.copy(path, zs);
              zs.closeEntry();
            } catch (IOException e) {
              System.err.println(e);
            }
          });
    }
  }
}
