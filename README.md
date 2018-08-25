# xetra-analysis
Assignment code for Oxford CLO module (July 2018)

## Usage

### Start Spark

To run new jobs, first you must start the Spark cluster. The `docker-compose.yml` file contains a definition for a Spark
master and a Spark worker.

Start the entire cluster with:

```bash
$ docker-compose up
```

By default the cluster will start with a single worker. You can create multiple workers using the `--scale` argument:

```bash
$ docker-compose up --scale spark-worker=5  # For 5 workers
```

Once the cluster is started, the Spark Web UI can be accessed at `http://localhost:4040`.

### Running Analysis

Run one-time analysis with the `./xetra-analysis.py` script.
