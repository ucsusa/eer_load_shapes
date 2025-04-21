# EER Load Shapes

This repository holds the workflow for producing load shapes with data from EER for use with the ReEDS-2.0 model.

In order to use the resulting load shapes. The desired scenario files must be copied to the `ReEDS-2.0/inputs/load` directory.

## Set up
After cloning this repository, set up a Conda environment using 
```bash
conda env create
activate eer-process
```

> [NOTE!]
> The workflow requires users have [7zip](https://www.7-zip.org/download.html) installed.

## Workflow Steps

1. Update the file `UCS_load_profile_scaling/scaling_inputs_MWh.csv`:

    This file contains the columns
    
    * `scenario`: The relevant EER scenario to modify
    * `subsector_group`: The subsectors to group together and rescale. E.g., "data center cooling" and "data center IT".
    * `year`: The modeled year
    * A column for each state + Washington, D.C. with values representing the total load for the combined subsectors identified in `subsector_group` in MWh.

2. Run the script `UCS_load_profile_scaling/main.py` in your terminal or command prompt with 
    ```bash
    python main.py
    ```

    The resulting files will be stored in a new folder called `UCS_load_profile_scaling/scaled_shapes`.

3. Run the script `eer_to_reeds_UCS.py` with 
    ```bash
    python eer_to_reeds_UCS.py`
    ```

    This script takes scaled input data from EER and 


## Credits

The script in `UCS_load_profile_scaling` was courtesy of [@ryandrewjones](github.com/ryandrewjones) from Evolved Energy Research.