# UCS_load_profile_scaling
The UCS Load Profile Scaling Tool is a command-line utility that scales 8760 hourly energy load profiles according to specified scaling factors. It reads CSV files containing hourly timeseries data, applies scaling transformations based on input parameters, and outputs the scaled data along with summary files.

Dependencies
This script requires the following Python packages:

Python 3.6+
pandas
numpy

Setup
1. Place your unscaled shape files in the unscaled_shapes/shape_outputs directory, organized by scenario and year
2. Create a scaling inputs file (default: scaling_inputs_MWh.csv) with your desired scaling factors

Directory Structure

UCS_load_profile_scaling/
│
├── main.py                      # The scaling script
├── scaling_inputs_MWh.csv       # Scaling factors by scenario, subsector group, year, and state
│
├── unscaled_shapes/
│   └── shape_outputs/
│       ├── scenario1/
│       │   ├── 2025.csv.gz
│       │   ├── 2030.csv.gz
│       │   └── summary_shapes.csv
│       │
│       └── scenario2/
│           ├── 2025.csv.gz
│           └── ...
│
└── scaled_shapes/               # Output directory (created by the script)
    ├── scenario1/
    │   ├── 2025.csv.gz
    │   ├── 2030.csv.gz
    │   └── summary_shapes.csv
    │
    └── scenario2/
        ├── 2025.csv.gz
        └── ...

Using the Script
Basic Usage
Run the script with default parameters:

python main.py

This will:

Read the unscaled data from unscaled_shapes/shape_outputs
Apply scaling factors from scaling_inputs_MWh.csv
Output scaled data to scaled_shapes

Custom Paths
You can customize the input and output paths:

python main.py --input-dir /path/to/unscaled/data --output-dir /path/to/output --scaling-inputs /path/to/scaling_inputs.csv