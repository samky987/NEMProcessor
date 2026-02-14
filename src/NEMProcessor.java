import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.time.format.*;
import java.util.concurrent.*;

public class NEMProcessor {

    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final String POISON_PILL = "EOF"; //The signal to stop threads
    private static final DateTimeFormatter SQL_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws InterruptedException {
        long startTime = System.nanoTime();

        // Queue for producer-consumer
        BlockingQueue<String> queue = new ArrayBlockingQueue<>(1000);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        String inputFile = "New Text Document.txt";

        //Start consumer threads
        startConsumerThreads(executor, queue);

        //Read NEM file and group NMI blocks
        processFile(inputFile, queue);

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        System.out.println("Execution Time: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");
    }

    private static void startConsumerThreads(ExecutorService executor, BlockingQueue<String> queue) {
        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> processConsumer(queue));
        }
    }

    private static void processConsumer(BlockingQueue<String> queue) {
        StringBuilder allRows = new StringBuilder(); //Accumulate all rows
        try {
            while (true) {
                String block = queue.take();
                if (block.equals(POISON_PILL)) {
                    break;
                }
                processNEM300BlockToBuffer(block, allRows);
            }

            if (allRows.length() > 2) { //remove trailing comma
                allRows.setLength(allRows.length() - 2);
            }

            if (allRows.length() > 0) {
                writeSqlToFile(allRows.toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processFile(String inputFile, BlockingQueue<String> queue) {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputFile))) {
            String line;
            StringBuilder currentBlock = new StringBuilder();

            //Read and process the file line by line
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("200")) {
                    if (!currentBlock.isEmpty()) queue.put(currentBlock.toString());
                    currentBlock.setLength(0);
                }
                currentBlock.append(line).append("\n");
            }

            if (!currentBlock.isEmpty()) queue.put(currentBlock.toString());

            // Place poison pills to stop all threads
            for (int i = 0; i < THREAD_COUNT; i++) {
                queue.put(POISON_PILL);
            }

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void processNEM300BlockToBuffer(String block, StringBuilder sb) {
        String[] lines = block.split("\n");
        String currentNmi = "";
        int intervalLength = 30;

        for (String line : lines) {
            if (line.startsWith("200")) {
                String[] parts = line.split(",");
                currentNmi = parts[1];
                intervalLength = Integer.parseInt(parts[8]);
            } else if (line.startsWith("300")) {
                parseNEM300Line(line, currentNmi, intervalLength, sb);
            }
        }
    }

    private static void parseNEM300Line(String line, String nmi, int interval, StringBuilder sb) {
        String[] cols = line.split(",");
        if (cols.length < 3) return;

        LocalDate baseDate = LocalDate.parse(cols[1], DateTimeFormatter.BASIC_ISO_DATE);

        LocalDateTime baseTime = baseDate.atStartOfDay();
        int expectedIntervals = 1440 / interval;

        for (int i = 0; i < expectedIntervals; i++) {
            int colIndex = i + 2;
            if (colIndex >= cols.length) break;

            String val = cols[colIndex].trim();
            if (val.isEmpty() || "0".equals(val)) continue;

            LocalDateTime time = baseTime.plusMinutes((long) (i + 1) * interval);

            sb.append("('").append(nmi).append("','").append(time.format(SQL_FORMAT))
            .append("',").append(val).append("),\n");
        }
    }

    private static void writeSqlToFile(String sqlRows) {
        try (BufferedWriter writer = Files.newBufferedWriter(
                Paths.get("meter_readings_" + Thread.currentThread().getName() + ".sql"),
                StandardCharsets.UTF_8)) {
            writer.write("INSERT INTO meter_readings (\"nmi\", \"timestamp\", \"consumption\") VALUES\n");
            writer.write(sqlRows);
            writer.write(";\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
