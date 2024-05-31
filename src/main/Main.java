package main;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class Main {

  private static final String SEARCH_COLUMN_HEADER = "Invoice #";
  private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

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
      LOGGER.log(Level.SEVERE, "An error occurred: ", e);
    }
  }

  private static Map<String, Boolean> readSearchStrings(final String csvFile) throws IOException {
    final Map<String, Boolean> searchStrings = new ConcurrentHashMap<>();
    try (final FileReader reader = new FileReader(csvFile)) {
      final Iterable<CSVRecord> records = CSVFormat.Builder.create().setHeader().build().parse(reader);

      for (final CSVRecord record : records) {
        final String searchString = record.get(SEARCH_COLUMN_HEADER).trim().toLowerCase();
        if (isEnglish(searchString)) {
          searchStrings.put(searchString, Boolean.TRUE);
        }
      }
    } catch (final IOException e) {
      LOGGER.log(Level.SEVERE, "Error reading CSV file: " + csvFile, e);
      throw e;
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

  private static Map<String, Set<String>> searchFiles(final String rootDir, final Map<String, Boolean> searchStrings, final int numWorkers) throws InterruptedException, ExecutionException {
    final Map<String, Set<String>> matchingFiles = new ConcurrentHashMap<>();
    final ExecutorService executorService = Executors.newFixedThreadPool(numWorkers);
    final List<Future<Map<String, Set<String>>>> futures = new ArrayList<>();

    try {
      Files.walk(Paths.get(rootDir))
              .filter(Files::isRegularFile)
              .filter(path -> path.toString().toLowerCase().endsWith(".pdf"))
              .forEach(path -> futures.add(executorService.submit(() -> processFile(path, searchStrings))));
    } catch (final IOException e) {
      LOGGER.log(Level.SEVERE, "Error walking through directory: " + rootDir, e);
    }

    for (final Future<Map<String, Set<String>>> future : futures) {
      try {
        final Map<String, Set<String>> result = future.get();
        result.forEach((key, value) -> matchingFiles.merge(key, value, (existing, newValue) -> {
          existing.addAll(newValue);
          return existing;
        }));
      } catch (final ExecutionException e) {
        LOGGER.log(Level.SEVERE, "Error processing file", e);
      }
    }

    executorService.shutdown();
    return matchingFiles;
  }

  private static Map<String, Set<String>> processFile(Path filePath, Map<String, Boolean> searchStrings) throws IOException {
    Map<String, Set<String>> matches = new HashMap<>();
    String filenameLower = filePath.getFileName().toString().toLowerCase();
    List<String> fileContent = readPdfContents(filePath);

    for (String searchString : searchStrings.keySet()) {
      String regex = "\\b" + Pattern.quote(searchString) + "\\b"; // Ensure the search string is a whole word
      Pattern pattern = Pattern.compile(regex);

      for (String line : fileContent) {
        if (pattern.matcher(line).find()) {
          matches.computeIfAbsent(searchString, k -> new HashSet<>()).add(filePath.toString());
          break;
        }
      }
    }

    return matches;
  }

  private static List<String> readPdfContents(Path filePath) throws IOException {
    List<String> content = new ArrayList<>();
    try (PDDocument document = PDDocument.load(filePath.toFile())) {
      PDFTextStripper stripper = new PDFTextStripper();
      String text = stripper.getText(document);
      String[] lines = text.split("\\r?\\n");
      content.addAll(Arrays.asList(lines));
    }
    return content;
  }

  private static void copyFiles(final Map<String, Set<String>> matchingFiles, final String targetDir) {
    matchingFiles.forEach((searchString, files) -> {
      files.forEach(filePath -> {
        try {
          final Path targetSubDir = Paths.get(targetDir);
          Files.createDirectories(targetSubDir);

          final Path sourcePath = Paths.get(filePath);
          final String baseName = sourcePath.getFileName().toString();
          final String name = baseName.substring(0, baseName.lastIndexOf('.'));
          final String ext = baseName.substring(baseName.lastIndexOf('.'));
          final Path targetFilePath = targetSubDir.resolve(searchString + "_" + name + ext);

          if (!Files.exists(targetFilePath)) {
            Files.copy(sourcePath, targetFilePath);
          }
        } catch (final IOException e) {
          LOGGER.log(Level.WARNING, "Error copying file: " + filePath, e);
        }
      });
    });
  }

  private static void writeUnmatchedSearchStrings(final Map<String, Boolean> searchStrings, final Map<String, Set<String>> matchingFiles, final String outputFile) {
    try (final FileWriter writer = new FileWriter(outputFile)) {
      for (final String searchString : searchStrings.keySet()) {
        if (!matchingFiles.containsKey(searchString)) {
          writer.write(searchString + System.lineSeparator());
        }
      }
    } catch (final IOException e) {
      LOGGER.log(Level.SEVERE, "Error writing unmatched search strings to file: " + outputFile, e);
    }
  }
}
