# HDFS Partitioning and Replication (Loan Data)

This project demonstrates how HDFS (Hadoop Distributed File System) partitions and replicates data for fault tolerance and scalability. A mini HDFS cluster is deployed using Docker, and Python is used to interact with the system through both the WebHDFS API and PyArrow. The project also explores the impact of replication on fault tolerance and space efficiency by simulating node failure.

## 📚 Learning Objectives

- Use the HDFS CLI to upload and manage files  
- Interact with HDFS using the WebHDFS REST API  
- Analyze HDFS files using PyArrow in Python  
- Understand HDFS block replication and its effect on space and reliability  
- Simulate partial data loss and attempt recovery  

---

## 🧱 Project Structure

- **Dockerized Cluster**: Mini HDFS cluster with NameNode, two DataNodes, and a JupyterLab container.
- **Data Upload**: A large CSV file is downloaded, converted to Parquet, and uploaded to HDFS with different replication levels.
- **WebHDFS Queries**: Python and `requests` used to query file status and block distribution.
- **PyArrow Analysis**: Files are read from HDFS using PyArrow, with performance comparison between full read and column-only read.
- **Disaster Recovery**: A DataNode is killed to simulate failure, and recovery of replicated vs non-replicated blocks is analyzed.

---

## 📁 Files and Notebooks

- `p4a.ipynb`: Notebook for HDFS upload, WebHDFS interaction, and PyArrow analysis  
- `p4b.ipynb`: Notebook for failure simulation and recovery analysis  
- `docker-compose.yml`: Defines the HDFS cluster setup  
- `hdfs.Dockerfile`, `namenode.Dockerfile`, `datanode.Dockerfile`, `notebook.Dockerfile`: Dockerfiles to build cluster components  
- `autograde.py`: Script to test and validate your implementation  
- `score.json`: Output of autograder  
- `.gitignore`: Prevents large datasets from being tracked by Git  

---

## ⚙️ Setup Instructions

1. **Clone and Build Docker Images**

   ```bash
   docker build . -f hdfs.Dockerfile -t p4-hdfs
   docker build . -f namenode.Dockerfile -t p4-nn
   docker build . -f datanode.Dockerfile -t p4-dn
   docker build . -f notebook.Dockerfile -t p4-nb

2. **Launch the Cluster**

   ```bash
   docker compose up -d

3. **Access JupyterLab**

   - Open: http://localhost:5000/lab
   - Run p4a.ipynb for Q1–Q8
   - Run p4b.ipynb for Q9–Q10

4. **Clean Up**

   ```bash
   docker compose kill
   docker compose rm -f

5. **📦 Data Used**

   - Source: [HDMA Wisconsin Loan Data 2021](https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.csv) (Download manuallly since it's not tracked by git)

   - File Types: CSV converted to Parquet for efficient processing
     
6. **📝 Notes**

   - Make sure Docker is configured to run without sudo
   - Ensure all .csv and .parquet files are excluded from Git with .gitignore
   - The cluster must be fully running before uploading files to ensure proper DataNode distribution

  

## 💻 Jupyter Notebook Tasks
All answers are recorded in `p5.ipynb` with corresponding question headers as comments (e.g., `#q1`, `#q2`, ...).

### Part 1: Deployment and Data Upload

**Q1**: How many live DataNodes are in the cluster?  
Run a shell command using `hdfs dfsadmin` and report the number of live DataNodes.

**Q2**: What are the logical and physical sizes of the parquet files?  
Use `hdfs dfs -du` to compare logical vs physical sizes of:
- `hdfs://boss:9000/single.parquet` (replication=1)
- `hdfs://boss:9000/double.parquet` (replication=2)

---

### Part 2: WebHDFS

**Q3**: What is the file status for `single.parquet`?  
Use the `GETFILESTATUS` WebHDFS operation and return the JSON dictionary.

**Q4**: What is the location for the first block of `single.parquet`?  
Use the `OPEN` operation with `offset=0` and `noredirect=true`.

**Q5**: How are the blocks of `single.parquet` distributed across the two DataNode containers?  
Parse the redirect URLs and summarize the number of blocks per container ID.

**Q6**: How are the blocks of `double.parquet` distributed across the two DataNode containers?  
Use the `GETFILEBLOCKLOCATIONS` WebHDFS operation to determine the block-to-host mapping.

---

### Part 3: PyArrow

**Q7**: What is the average `loan_amount` in the `double.parquet` file?  
Use PyArrow to read the full file from HDFS and compute the average.

**Q8**: How much faster is it to read only the `loan_amount` column vs the full file?  
Compare the time taken in Q7 and this column-specific read. Report the ratio.

---

### Part 4: Disaster Strikes

**Q9**: How many live DataNodes are in the cluster after manually killing one?  
Re-run the DataNode count as in Q1 — should now report only 1 live DataNode.

**Q10**: How many blocks of `single.parquet` were lost due to DataNode failure?  
Use `GETFILEBLOCKLOCATIONS` or failed `OPEN` attempts to detect lost blocks.

---

