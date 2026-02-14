
## ** What is the rationale for the technologies you have decided to use?**

- BufferedReader & BufferedWriter used for efficient reading and writing operations. 

- BlockingQueue & ExecutorService are used to decouple the I/O-bound task from the CPU-bound task. This allows the parsing logic to run in parallel and utilize available CPU cores

- StringBuilder to accumulate SQL `INSERT` statements in batches. Unlike regular string concatenation, `StringBuilder` does not create new objects for each append operation, like vanilla String.


---

## ** What would you have done differently if you had more time?**

- Replace `String.split()` logic with manual parsing or a dedicated CSV/NEM parsing library
- Load data directly into a database using **COPY** statements, instead of creating an intermediary SQL file.
- Implement more error handling for invalid data.
- I would look into distributed data processing using **Apache Spark**.
- Add unit tests to verify correctness of parsing and SQL generation logic.
- Replace `System.out` statements with a logging framework.
- Benchmark different parsing approaches and libraries to identify the most efficient solution.

---

## ** What is the rationale for the design choices that you have made?**

- I assumed that zero consumption values can be ignored to improve performance. This can be changed easily if required.
- Poison Pill pattern to ensures a graceful shutdown of the thread pool. Each consumer finishes processing and exits only after the file is fully processed.
- StringBuilder for SQL generation, reduces memory overhead and avoids unnecessary object creation during string concatenation.
- BlockingQueue and producer-consumer pattern allow parallel processing. While one thread is performing I/O operations, other threads process data simultaneously, improving overall efficiency.
- Line breaks in the 300-record dataset are handled via block based data aggregation