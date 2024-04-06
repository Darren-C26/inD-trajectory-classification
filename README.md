# inD Trajectory Classification

## Introduction
This project aims to analyze and classify trajectories of objects recorded at various intersections. By processing data from CSV files containing track records, the pipeline computes trajectory classifications such as 'Parked,' 'Straight,' 'Left Turn,' and 'Right Turn.' The resulting insights can be used for traffic analysis, urban planning, and safety assessment.

## Setup and Configuration
### Prerequisites
- Python 3.x
- Google Cloud Platform account
- Google Cloud SDK installed and configured

### Installation
1. Clone the repository: `git clone https://github.com/Darren-C26/inD-trajectory-classification`
2. Install dependencies: `pip install -r requirements.txt`
3. Set up Google Cloud credentials: Follow the instructions [here](https://cloud.google.com/docs/authentication/getting-started)
4. Configure environment variables if necessary.

## Architecture
The project utilizes Google Cloud Platform services such as Google Cloud Storage, BigQuery, and Pub/Sub. The data pipeline is built using Apache Beam for data processing and streaming. Messages are published to Pub/Sub topics based on trajectory classifications, and processed records are saved to BigQuery for further analysis.

## Pipeline Workflow
1. **Data Ingestion:** CSV files containing track records are ingested from Google Cloud Storage.
2. **Data Processing:** Records are parsed and grouped by a composite key consisting of recordingId and trackId. Trajectories are computed based on object movements, and average velocity and acceleration metrics are calculated.
3. **Data Storage:** Processed records are saved to BigQuery with a defined schema.
4. **Message Publishing:** Messages are published to Pub/Sub topics based on trajectory classifications ('Parked,' 'Straight,' 'Left Turn,' 'Right Turn').
5. **Looker Studio Report:** Insights from the processed data are visualized using Looker Studio, providing comprehensive analytics and visualization of object trajectories across various intersection scenarios.

## Looker Studio Report
Access the Looker Studio report [here](https://lookerstudio.google.com/s/oabCVkyYdxg). Explore general analytics of object trajectories for critical scenario analysis at intersections.

## Implementation Demo
The following video provides a demonstration of the trajectory classification pipeline in action, showcasing how object trajectories are computed and classified based on various intersection scenarios.

[Click to view demo](https://drive.google.com/file/d/1anP9tobmOz5Bw65v38ZkjOnBAQNttaOV/preview)