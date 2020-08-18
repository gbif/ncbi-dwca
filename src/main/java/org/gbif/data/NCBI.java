package org.gbif.data;

import org.apache.commons.io.FileUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.*;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * https://ftp.ncbi.nih.gov/pub/taxonomy/taxdump_readme.txt
 *
 * The name type has 13 distinct values:
 *
 * acronym -> 2760819 | EV-C117 |  | acronym
 * in-part -> 2203421 | Dictyocheirospora sp. YJ-2018a |  | in-part
 * includes -> 2762514 | Fomitiporia sp. 7 GAS-2016 |  | includes
 * common name -> 2759916 | Unterstein's newt |  | common name
 * genbank common name -> 2762440 | Mohawk Dunes fringe-toed lizard |  | genbank common name
 * blast name -> 2558200 | hawks & eagles |  | blast name
 * scientific name -> 2763003 | Agrypninae incertae sedis |  | scientific name
 * synonym -> 2762697 | Pterocarya insignis |  | synonym
 * type material -> 2762514 | BAFC 24382 | BAFC 24382 <holotype> | type material
 * genbank synonym -> 2653933 | Citrobacter sp. 6106 |  | genbank synonym
 * authority -> 2762697 | Pterocarya macroptera var. insignis (Rehder & E.H.Wilson) W.E.Manning, 1975 |  | authority
 * genbank acronym -> 2758139 | BatPyV5b-1 |  | genbank acronym
 * equivalent name -> 2761759 | Rubus pirifolius var. permollis |  | equivalent name
 */
public class NCBI {

  static final URI LOCATION = URI.create("https://ftp.ncbi.nih.gov/pub/taxonomy/taxdmp.zip");
  static final String TAXA = "nodes.dmp";
  static final String NAMES = "names.dmp";
  static final Pattern SPLITTER = Pattern.compile("\\s*\\|\\s*");

  private final File dir;
  private final Map<Integer, NameUsage> usages;
  private final Map<String, String> types = new HashMap<>();
  private final DB db;

  private String[] _row;

  public NCBI(File dir) throws IOException {
    this.dir = dir;
    if (dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
    File dbf = new File(dir, "mapdb");
    dbf.getParentFile().mkdirs();
    if (dbf.exists()) {
      dbf.delete();
    }
    db = DBMaker
        .fileDB(dbf)
        .make();
    usages = db.hashMap("usages")
        .keySerializer(Serializer.INTEGER)
        .valueSerializer(Serializer.JAVA)
        .create();
  }

  public void run() throws IOException, InterruptedException {
    export();
    System.exit(0);


    System.out.println("Download NCBI dump from " + LOCATION);
    HttpRequest request = HttpRequest.newBuilder()
        .uri(LOCATION)
        .GET()
        .build();
    try {
      HttpResponse<InputStream> response = HttpClient
          .newBuilder()
          .proxy(ProxySelector.getDefault())
          .followRedirects(HttpClient.Redirect.ALWAYS)
          .build()
          .send(request, HttpResponse.BodyHandlers.ofInputStream());
      System.out.println("Response " + response.statusCode() + ": " + response);

      ZipInputStream stream = new ZipInputStream(response.body());
      System.out.println("Got the ZIP stream");
      ZipEntry entry = stream.getNextEntry();
      while (entry != null) {
        System.out.println("\n\n*** " + entry.getName() + " ***");
        if (TAXA.equalsIgnoreCase(entry.getName())) {
          extract(stream, this::taxa);
          stream.closeEntry();
          break;
        } else if (NAMES.equalsIgnoreCase(entry.getName())) {
          extract(stream, this::names);
          stream.closeEntry();
        }
        entry = stream.getNextEntry();
      }
      export();
      System.out.println("done.\n");

    } finally {
      System.out.println("All " + types.size() + " encountered types:");
      for (Map.Entry<String, String> t : types.entrySet()) {
        System.out.println(t.getKey() + " -> " + t.getValue());
      }
      db.close();
    }
  }

  static void _write(FileWriter writer, Integer... cols) throws IOException {
    writer.append(Arrays.stream(cols).map(NCBI::str).collect(Collectors.joining("\t")) + "\t");
  }
  static void write(FileWriter writer, String... cols) throws IOException {
    writer.append(Arrays.stream(cols).map(NCBI::str).collect(Collectors.joining("\t")) + "\n");
  }
  static void write(FileWriter writer, Integer key, String... cols) throws IOException {
    _write(writer, key);
    write(writer, cols);
  }
  static void write(FileWriter writer, Integer key, Integer parentKey, String... cols) throws IOException {
    _write(writer, key, parentKey);
    write(writer, cols);
  }
  static String str(Integer x) {
    return x == null ? "" : x.toString();
  }
  static String str(String x) {
    return x == null ? "" : x;
  }

  void export() throws IOException {
    System.out.println("Writing DwC text file ...");
    try (
        FileWriter taxa = new FileWriter(new File(dir, "taxa.txt"), StandardCharsets.UTF_8);
        FileWriter vernacular = new FileWriter(new File(dir, "vernacular.txt"), StandardCharsets.UTF_8);
    ) {
      // taxa, "taxonID", "parentNameUsageID", "acceptedNameUsageID", "taxonRank", "scientificName", "taxonRemarks";
      // vernacular, "taxonID", "vernacularName"
      for (NameUsage u : usages.values()) {
        write(taxa, u.key, u.parentKey, null, u.rank, u.name, u.comments);
        for (String v : u.vernacular) {
          write(vernacular, u.key, v);
        }
        int x = 1;
        for (String s : u.synonyms) {
          write(taxa, u.key+"-s" + x++, null, String.valueOf(u.key), null, s, null);
        }
      }
      Utils.copy("/ncbi/meta.xml", new File(dir, "meta.xml"));
    }
    File zip = new File(dir, "ncbi.zip");
    System.out.println("Packing ZIP archive at ");
    Utils.zip(dir.toPath(), zip.toPath(), "mapdb", zip.getName());
  }

  void extract(InputStream nodes, Consumer<NameUsage> consumer) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(nodes, StandardCharsets.UTF_8));
    int counter=0;
    for (String line = br.readLine(); line != null; line = br.readLine()) {
      //System.out.println(line);
      _row = SPLITTER.split(line);
      if (_row != null && _row.length > 0) {
        int key = Integer.parseInt(row(0));
        NameUsage u = usages.getOrDefault(key, new NameUsage());
        u.key = key;
        consumer.accept(u);
        usages.put(key, u);
        counter++;
        if (counter % 10000 == 0) {
          System.out.println("Added "+counter+" usages");
        }
      }
    }
  }

  void taxa(NameUsage u) {
    u.parentKey = Integer.parseInt(col(1,0));
    u.hidden = Integer.parseInt(row(10)) == 1;
    u.rank = row(2);
    u.comments = row(12);
  }

  void names(NameUsage u) {
    String name = row(1);
    String unique = row(2);
    String type = row(3);
    types.put(type, String.join(" | ", _row));
    if (type != null ) {
      switch (type) {
        case "authority":
          u.name = name;
          break;
        case "scientific name":
          if (u.name == null) {
            u.name = name;
          }
          break;
        case "synonym":
          u.synonyms.add(name);
          break;
        case "equivalent name":
          u.synonyms.add(name);
          break;
        case "common name":
          u.vernacular.add(name);
          break;
        case "type material":
          // what to do with that???
          break;
      }
    }
    //System.out.println(String.format("Type=%s; name=%s; unique=%s", type ,u.name, unique));
  }

  String row(int col) {
    return _row.length > col ? _row[col].trim() : null;
  }

  String col(int... cols) {
    if (_row != null && _row.length > 1) {
      for (int col : cols) {
        String v = row(col);
        if (v != null) return v;
      }
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    File dir = new File("output");
    new NCBI(dir).run();
  }
}
