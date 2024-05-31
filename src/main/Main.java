package main;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {

  private static final String SEARCH_COLUMN_HEADER = "Invoice #";

  public static void main(final String[] args) {
    if (args.length != 5) {
      System.out.println("Usage: java PDFSearchAndCopy <csvFile> <rootDir> <targetDir> <outputFile> <numWorkers>");
      System.exit(1);
    }

    final String csvFile = args[0];
    final String rootDir = args[1];
    final String targetDir = args[2];
    final String outputFile = args[3];
    final int numWorkers = Integer.parseInt(args[4]);

    try {
      final Map<String, Boolean> searchStrings = readSearchStrings(csvFile);
      final Map<String, Set<String>> matchingFiles = searchFiles(rootDir, searchStrings, numWorkers);
      copyFiles(matchingFiles, targetDir);
      writeUnmatchedSearchStrings(searchStrings, matchingFiles, outputFile);
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  private static Map<String, Boolean> readSearchStrings(final String csvFile) throws IOException {
    final Map<String, Boolean> searchStrings = new ConcurrentHashMap<>();
    final FileReader reader = new FileReader(csvFile);
    final Iterable<CSVRecord> records = CSVFormat.Builder.create().setHeader().build().parse(reader);
//    CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
//    String headerName = null;
//
//    // Find the "Invoice" column header
//    for (final String header : records.iterator().next().toMap().keySet()) {
//      if (header.contains(SEARCH_COLUMN_HEADER)) {
//        headerName = header;
//        break;
//      }
//    }
//
//    if (headerName == null) {
//      throw new IllegalArgumentException("No column containing 'Invoice' found");
//    }

    for (final CSVRecord csvRecord : records) {
      final String searchString = csvRecord.get(SEARCH_COLUMN_HEADER).trim().toLowerCase();
      if (isEnglish(searchString)) {
        searchStrings.put(searchString, Boolean.TRUE);
      }
    }

    return searchStrings;
  }

  private static boolean isEnglish(final String text) {
    for (final char c : text.toCharArray()) {
      if (c >= 128) {
        return false;
      }
    }
    return true;
  }

  private static Map<String, Set<String>> searchFiles(final String rootDir, final Map<String, Boolean> searchStrings, final int numWorkers) throws InterruptedException, ExecutionException, IOException {
    final Map<String, Set<String>> matchingFiles = new ConcurrentHashMap<>();
    final ExecutorService executorService = Executors.newFixedThreadPool(numWorkers);
    final List<Future<Map<String, Set<String>>>> futures = new ArrayList<>();

    Files.walk(Paths.get(rootDir))
            .filter(Files::isRegularFile)
            .filter(path -> path.toString().toLowerCase().endsWith(".pdf"))
            .forEach(path -> futures.add(executorService.submit(() -> processFile(path, searchStrings))));

    for (final Future<Map<String, Set<String>>> future : futures) {
      final Map<String, Set<String>> result = future.get();
      result.forEach((key, value) -> matchingFiles.merge(key, value, (existing, newValue) -> {
        existing.addAll(newValue);
        return existing;
      }));
    }

    executorService.shutdown();
    return matchingFiles;
  }

  private static Map<String, Set<String>> processFile(final Path filePath, final Map<String, Boolean> searchStrings) throws IOException {
    final Map<String, Set<String>> matches = new HashMap<>();
    final String filenameLower = filePath.getFileName().toString().toLowerCase();
    final String fileContent = readPdfContents(filePath);

    for (final String searchString : searchStrings.keySet()) {
      if (filenameLower.contains(searchString) || fileContent.contains(searchString)) {
        matches.computeIfAbsent(searchString, k -> new HashSet<>()).add(filePath.toString());
      }
    }

    return matches;
  }

  private static String readPdfContents(final Path filePath) {
    try (final PDDocument document = PDDocument.load(filePath.toFile())) {
      final PDFTextStripper stripper = new PDFTextStripper();
      return stripper.getText(document).toLowerCase();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return "";
  }

  private static void copyFiles(final Map<String, Set<String>> matchingFiles, final String targetDir) throws IOException {
    for (final String searchString : matchingFiles.keySet()) {
      for (final String filePath : matchingFiles.get(searchString)) {
        final Path targetSubDir = Paths.get(targetDir);
        Files.createDirectories(targetSubDir);
        final Path sourcePath = Paths.get(filePath);
        final String baseName = sourcePath.getFileName().toString();
        final String name = baseName.substring(0, baseName.lastIndexOf('.'));
        final String ext = baseName.substring(baseName.lastIndexOf('.'));
        final Path targetFilePath = targetSubDir.resolve(searchString.toUpperCase() + "_" + name + ext);

        if (!Files.exists(targetFilePath)) {
          Files.copy(sourcePath, targetFilePath);
        }
      }
    }
  }

  private static void writeUnmatchedSearchStrings(final Map<String, Boolean> searchStrings, final Map<String, Set<String>> matchingFiles, final String outputFile) throws IOException {
    try (final FileWriter writer = new FileWriter(outputFile)) {
      for (final String searchString : searchStrings.keySet()) {
        if (!matchingFiles.containsKey(searchString)) {
          writer.write(searchString + System.lineSeparator());
        }
      }
    }
  }}