# Dataplex Lineage CLI

The Dataplex Lineage CLI is a command-line interface tool for performing various operations related to Google Cloud Dataplex, focusing on lineage and event management.

## Features

- Emit lineage events
- Search for links between data assets
- Retrieve processes, runs, and events
- Query processes, runs, and events for specific links

## Prerequisites

- Java 8 or higher
- Google Cloud Project with Dataplex API enabled
- Appropriate permissions to access Dataplex resources

## Building the Project

This project uses Maven for dependency management and building. To build the project:

```sh
./gradlew package
```

## Usage

```sh
java -jar dataplex-lineage-cli.jar [OPTIONS]
```

### Available Commands

- `emit-ol-event`: Emit a lineage event
- `get-links`: Search for links between data assets
- `get-processes`: Retrieve processes for a project
- `get-runs`: Retrieve runs for a project or process
- `get-events`: Retrieve events for a project, process, or run
- `get-processes-for-link`: Get processes associated with a specific link
- `get-runs-for-link`: Get runs associated with a specific link
- `get-events-for-link`: Get events associated with a specific link

### Common Options

- `-c, --command`: Specify the command to execute (required)
- `-p, --project`: Google Cloud project ID (required)
- `-l, --location`: Location (defaults to 'us')
- `--credentials-file`: Path to the credentials file
- `--endpoint`: API endpoint
- `--pretty`: Pretty print JSON output

### Command-Specific Options

- `emit-ol-event`:
    - `-e, --event`: JSON string of the OpenLineage event to emit
    - `--mode`: Operation mode: 'sync' or 'async'

- `get-links`:
    - `--source`: Source for link search
    - `--target`: Target for link search

- `get-events`, `get-runs`:
    - `--ol-job-namespace`: OpenLineage job namespace
    - `--ol-job-name`: OpenLineage job name
    - `--process`: Process name
    - `--run`: Run name

- `get-processes-for-link`, `get-runs-for-link`, `get-events-for-link`:
    - `--link`: Link for processes, runs, or events queries

## Examples

1. Emit an event:

```sh
java -jar dataplex-lineage-cli.jar -c emit-event --project your-project-id -l us-central1 -e '{...}'
```

2. Search for links:

```sh
java -jar dataplex-lineage-cli.jar -c get-links --project your-project-id -l us-central1 --source "gcs:example-bucket.warehouse/transactions"
```

3. Get events for a specific OpenLineage job:

```sh
java -jar dataplex-lineage-cli.jar -c get-events --project your-project-id -l us-central1 --ol-job-namespace your-namespace --ol-job-name your-job-name
```

# Disclaimer

This is not an officially supported Google project.
