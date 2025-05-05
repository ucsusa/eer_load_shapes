import os
import argparse
import pandas as pd
import numpy as np
import pdb
from pathlib import Path


def interpolate_scaling_factors(scaling_inputs, scenario, subsector_group, target_year, available_years):
    """
    Interpolate scaling factors if the target year isn't in the scaling inputs
    """
    # Filter for the specific scenario and subsector_group
    scenario_data = scaling_inputs[(scaling_inputs['scenario'] == scenario) & 
                                  (scaling_inputs['subsector_group'] == subsector_group)]
    
    if scenario_data.empty:
        print(f"Warning: No scaling data found for scenario '{scenario}' and subsector group '{subsector_group}'")
        # Return 1.0 for all states (no scaling)
        return {state: 1.0 for state in scaling_inputs.columns[3:]}
    
    # Get the years in the scaling inputs for this scenario/group
    input_years = sorted(scenario_data['year'].unique())
    
    if target_year in input_years:
        # Direct match - no interpolation needed
        year_data = scenario_data[scenario_data['year'] == target_year].iloc[0]
        return {state: year_data[state] for state in scaling_inputs.columns[3:]}
    
    # Need to interpolate
    if target_year < min(input_years):
        # Target year is before first year in inputs - use earliest year
        year_data = scenario_data[scenario_data['year'] == min(input_years)].iloc[0]
        print(f"Warning: Target year {target_year} is before first scaling input year. Using data from {min(input_years)}.")
        return {state: year_data[state] for state in scaling_inputs.columns[3:]}
    
    if target_year > max(input_years):
        # Target year is after last year in inputs - use latest year
        year_data = scenario_data[scenario_data['year'] == max(input_years)].iloc[0]
        print(f"Warning: Target year {target_year} is after last scaling input year. Using data from {max(input_years)}.")
        return {state: year_data[state] for state in scaling_inputs.columns[3:]}
    
    # Interpolate between the two closest years
    lower_year = max([y for y in input_years if y < target_year])
    upper_year = min([y for y in input_years if y > target_year])
    
    lower_data = scenario_data[scenario_data['year'] == lower_year].iloc[0]
    upper_data = scenario_data[scenario_data['year'] == upper_year].iloc[0]
    
    # Calculate the proportion for interpolation
    proportion = (target_year - lower_year) / (upper_year - lower_year)
    
    # Interpolate for each state
    scaling_factors = {}
    for state in scaling_inputs.columns[3:]:
        scaling_factors[state] = lower_data[state] + proportion * (upper_data[state] - lower_data[state])
    
    return scaling_factors


def scale_profile(df, scaling_factors, subsector_group):
    """
    Scale the 8760 timeseries based on scaling factors
    Handles zero-to-positive scaling case
    """
    # Create a copy to avoid modifying the original
    scaled_df = df.copy()
    
    # Extract the subsectors in this group
    subsectors = [s.strip() for s in subsector_group.split(',')]
    
    # Filter rows that belong to the subsector group
    mask = scaled_df['subsector'].isin(subsectors)
    
    # Get state columns (exclude non-state columns)
    state_columns = [col for col in scaled_df.columns if col not in ['sector', 'subsector', 'weather_datetime']]
    
    # For each state, perform scaling
    for state in state_columns:
        if state in scaling_factors:
            scaling_factor = scaling_factors[state]
            
            # Handle edge case 1: Initial energy may be zero but we want to scale to positive
            # Calculate the sum for this subsector group for this state
            group_sum = scaled_df.loc[mask, state].sum()
            
            if group_sum == 0 and scaling_factor > 0:
                # Zero-to-positive case: distribute evenly across all hours
                num_rows = mask.sum()
                if num_rows > 0:
                    scaled_df.loc[mask, state] = int(scaling_factor / num_rows)
                    print(f"  Zero-to-positive scaling applied for {state}, subsector_group: {subsector_group}")
            else:
                # Normal scaling case
                if group_sum != 0:
                    # Calculate ratio needed to achieve target scaling
                    ratio = scaling_factor / group_sum
                    scaled_df.loc[mask, state] = (scaled_df.loc[mask, state] * ratio).astype(int)
    
    return scaled_df


def create_original_energy_summary(unscaled_directory, scaling_inputs, output_dir):
    """
    Create a summary file with the original energy values before scaling,
    in the same format as scaling_inputs_MWh.csv
    """
    print("Generating original energy summary file...")
    
    # Initialize dataframe to store results
    result_data = []
    
    # Get unique scenarios and subsector groups from scaling inputs
    scenarios = scaling_inputs['scenario'].unique()
    
    for scenario in scenarios:
        print(f"  Processing scenario: {scenario}")
        
        # Get subsector groups for this scenario
        subsector_groups = scaling_inputs[scaling_inputs['scenario'] == scenario]['subsector_group'].unique()
        
        # Path to scenario directory
        scenario_directory = Path(unscaled_directory) / scenario
        
        # Process each year in the scenario directory
        for year_file in os.listdir(scenario_directory):
            if year_file == 'summary_shapes.csv' or not year_file.endswith('.csv.gz'):
                continue
                
            year = int(year_file.split('.')[0])
            print(f"    Processing year: {year}")
            
            # Read the unscaled data
            df = pd.read_csv(os.path.join(scenario_directory, year_file), compression='gzip')
            
            # Get state columns (exclude non-state columns)
            state_columns = [col for col in df.columns if col not in ['sector', 'subsector', 'weather_datetime']]
            
            # Process each subsector group
            for subsector_group in subsector_groups:
                # Extract the subsectors in this group
                subsectors = [s.strip() for s in subsector_group.split(',')]
                
                # Filter rows that belong to the subsector group
                mask = df['subsector'].isin(subsectors)
                
                # Skip if no rows match this subsector group
                if not mask.any():
                    print(f"      Warning: No data found for subsector group '{subsector_group}' in {year}")
                    # Add a row with zeros for all states
                    row_data = {
                        'scenario': scenario,
                        'subsector_group': subsector_group,
                        'year': year
                    }
                    for state in state_columns:
                        row_data[state] = 0
                    result_data.append(row_data)
                    continue
                
                # Calculate sum for each state for this subsector group
                row_data = {
                    'scenario': scenario,
                    'subsector_group': subsector_group,
                    'year': year
                }
                
                for state in state_columns:
                    # Get the total energy for this state and subsector group
                    total_energy = df.loc[mask, state].sum()
                    row_data[state] = total_energy
                
                result_data.append(row_data)
    
    # Create dataframe from results
    result_df = pd.DataFrame(result_data)
    
    # Save to CSV
    output_file = Path(output_dir) / 'original_energy_values.csv'
    result_df.to_csv(output_file, index=False)
    print(f"Original energy summary saved to {output_file}")


def generate_summary_file(scenario, scenario_data, output_dir):
    """
    Generate summary_shapes.csv for a scenario by summing across subsectors,
    with years in columns and states in rows
    """
    print(f"  Generating summary_shapes.csv for scenario: {scenario}")
    
    # Stack the dataframes and reformat
    summary_df = pd.concat(scenario_data.values(), keys=scenario_data.keys(), names=['year'])
    summary_df = summary_df.reset_index('year')
    del summary_df['subsector']
    summary_df = summary_df.groupby(['weather_datetime', 'year', 'sector']).sum()
    summary_df.columns.name = 'state'
    summary_df = summary_df.stack()
    summary_df = summary_df.unstack('year')
    summary_df = summary_df.reorder_levels(['weather_datetime', 'state', 'sector'])
    summary_df = summary_df.sort_index()
    
    # Convert year columns to integers
    summary_df.columns = [str(int(col)) for col in summary_df.columns]
    
    summary_df.to_csv(output_dir / 'summary_shapes.csv', index=True)
    print(f"  Created summary_shapes.csv with {len(summary_df)} rows")


def main(args):
    # Create output directory structure if it doesn't exist
    scaled_dir = Path(args.output_dir)
    scaled_dir.mkdir(exist_ok=True)
    
    # Read scaling inputs
    scaling_inputs = pd.read_csv(args.scaling_inputs)
    
    # Create original energy summary
    # create_original_energy_summary(args.input_dir, scaling_inputs, os.path.dirname(args.output_dir))
    
    # Dictionary to store data for summary shapes
    summary_data = {}
    
    # Process each scenario
    unscaled_directory = Path(args.input_dir)
    scenarios = [d for d in os.listdir(unscaled_directory) if os.path.isdir(unscaled_directory / d)]
    
    for scenario in scenarios:
        print(f"Processing scenario: {scenario}")
        scenario_directory = unscaled_directory / scenario
        
        # Get available years in the scenario directory (excluding summary_shapes.csv)
        available_years = [int(y.split('.')[0]) for y in os.listdir(scenario_directory) 
                          if y.endswith('.csv.gz') and y != 'summary_shapes.csv']
        
        # Create scenario directory in output if it doesn't exist
        scaled_scenario_dir = scaled_dir / scenario
        scaled_scenario_dir.mkdir(exist_ok=True)
        
        # Store scaled data for summary file generation
        scenario_data = {}
        
        # Process each year file
        for year_file in os.listdir(scenario_directory):
            if year_file == 'summary_shapes.csv':
                continue
                
            if not year_file.endswith('.csv.gz'):
                continue
            
            year = int(year_file.split('.')[0])
            print(f"  Processing year: {year}")
            
            # Read the unscaled data
            df = pd.read_csv(os.path.join(scenario_directory, year_file), compression='gzip')
            
            # Get unique subsector groups for this scenario from scaling inputs
            subsector_groups = scaling_inputs[scaling_inputs['scenario'] == scenario]['subsector_group'].unique()
            
            # Create a copy for scaled data (start with original)
            scaled_df = df.copy()
            
            # Process each subsector group
            for subsector_group in subsector_groups:
                print(f"    Scaling subsector group: {subsector_group}")
                
                # Get scaling factors for this scenario/subsector_group/year
                scaling_factors = interpolate_scaling_factors(
                    scaling_inputs, scenario, subsector_group, year, available_years
                )
                
                # Scale the profile for this subsector group
                scaled_df = scale_profile(scaled_df, scaling_factors, subsector_group)
            
            # Save scaled data
            output_file = scaled_scenario_dir / year_file
            scaled_df.to_csv(output_file, compression='gzip', index=False)
            
            # Store in memory for summary file
            scenario_data[year] = scaled_df
        
        # Store in overall summary dictionary
        summary_data[scenario] = scenario_data
        
        # Generate summary shapes file for this scenario
        generate_summary_file(scenario, scenario_data, scaled_scenario_dir)
    
    print("Processing complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scale 8760 load profiles based on scaling inputs')
    parser.add_argument('--input-dir', type=str, 
                        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'unscaled_shapes', 'shape_outputs'),
                        help='Directory containing unscaled shape outputs')
    parser.add_argument('--output-dir', type=str, 
                        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scaled_shapes'),
                        help='Directory to store scaled shape outputs')
    parser.add_argument('--scaling-inputs', type=str, 
                        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scaling_inputs_MWh.csv'),
                        help='CSV file with scaling inputs')
    
    args = parser.parse_args()
    main(args)
