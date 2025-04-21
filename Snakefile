import pandas as pd
import numpy as np
import subprocess

scenarios = ["current policy", "central high data center", "central"]
model_years = np.arange(2025,2055, 5)

historical_data_url = ("https://github.com/NREL/ReEDS-2.0/raw/refs/heads/"
                       "main/inputs/load/EER_IRAmoderate_load_hourly.h5")
county_to_ba_url = ("https://github.com/ucsusa/ReEDS-2.0/raw/refs/heads/"
                            "main/inputs/county2zone.csv")
load_participation_url = ("https://raw.githubusercontent.com/ucsusa/ReEDS-2.0/"
                          "refs/heads/main/hourlize/inputs/load/"
                          "load_participation_factors_st_to_ba.csv")

input_data_path = "input_data/"
results_path = "results/"

rule retrieve_historical_data:
    output: f"{input_data_path}EER_IRAmoderate_load_hourly.h5"
    shell:
        f"""
        curl -LO {input_data_path}{historical_data_url}
        """ 

rule retrieve_disaggregation_data:
    output: f"{input_data_path}county2zone.csv"
    run:
        df = pd.read_csv(county_to_ba_url)
        df.to_csv(str(output))


rule retrieve_load_participation:
    output: f"{input_data_path}load_participation_factors_st_to_ba.csv"
    run:
        df = pd.read_csv(load_participation_url)
        df.to_csv(str(output)) 


rule rescale_load_data:
    input:
        expand("UCS_load_profile_scaling/unscaled_shapes/shape_outputs/{scenario}/{year}.csv.gz",
        scenario=scenarios, year=model_years)
    output:
        expand("UCS_load_profile_scaling/scaled_shapes/{scenario}/{year}.csv.gz",
        scenario=scenarios, year=model_years)
    script:
        "UCS_load_profile_scaling/main.py"

rule unzip_scaled_data:
    input:
        expand("UCS_load_profile_scaling/scaled_shapes/{scenario}/{year}.csv.gz",
            scenario=scenarios, year=model_years)
    output:
        expand("input_data/{scenario}/{year}.csv",
            scenario=scenarios, year=model_years)
    run:
        for i, (infile, outfile) in enumerate(zip(input, output)):
            print(f"{infile} > {outfile}")
            outdir="/".join(outfile.split("/")[:-1])
            subprocess.run(f"7z x -o\"{outdir}\" \"{infile}\"")
        
rule build_dag:
    input: "Snakefile"
    output:
        "dag.png"
    shell:
        "snakemake --dag | dot -Tpng > {output}"