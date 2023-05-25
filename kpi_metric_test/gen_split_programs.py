import json
import random
from datetime import datetime
import pandas as pd
import os


def get_metric(program_name):
    json_file = os.path.dirname(os.path.abspath(__file__)) + f'/input/{program_name}_config.json'
    with open(json_file) as f:
        data = json.load(f)
    metrics = data[0]['input']['metrics']
    metric = list(metrics.keys())
    return metric


def dimension_list(program_name):
    grammar_csv = os.path.dirname(os.path.abspath(__file__)) + f'/input/{program_name}-event.grammar.csv'
    grammar_csv = pd.read_csv(grammar_csv, skiprows=3, nrows=2, header=None)
    dim_list = []
    for i in range(len(grammar_csv.columns)):
        for j in range(len(grammar_csv.index)):
            if grammar_csv.iloc[j, i] == "dimension":
                dim_list.append(grammar_csv.iloc[0, i])
    return dim_list

def time_dimension_list(program_name):
    grammar_csv = os.path.dirname(os.path.abspath(__file__)) + f'/input/{program_name}-event.grammar.csv'
    grammar_csv = pd.read_csv(grammar_csv, skiprows=3, nrows=2, header=None)
    tim_dim_list = []
    for i in range(len(grammar_csv.columns)):
        for j in range(len(grammar_csv.index)):
            if grammar_csv.iloc[j, i] == "timeDimension":
                tim_dim_list.append(grammar_csv.iloc[0, i])
    return tim_dim_list


def daily(dim_list, tim_dim_list, data_file, metric_file):
    data_file = pd.read_csv(data_file)
    for dim_col in dim_list:
        output_csv = os.path.dirname(os.path.abspath(__file__)) + f'/output/{dim_col}_{metric_file}_daily_output.csv'
        with open(output_csv, "w") as file:
            header = tim_dim_list + [dim_col] + ['sum'] + ['count'] + ['avg']
            file.write(",".join(header) + "\n")
            for i, row in data_file.drop_duplicates(subset=dim_col).iterrows():
                data_row = []
                for col in header:
                    if col in tim_dim_list:
                        data_row.append(row[col])
                    elif col == dim_col:
                        data_row.append(row[dim_col])

                file.write(",".join([str(x) for x in data_row]) + "\n")
        print(f"Output file {output_csv} created successfully")
    return True

def yearly(dim_list,data_file, metric_file):
    data_file = pd.read_csv(data_file)
    year_col = [data_file.columns[-1]]

    for dim_col in dim_list:
        output_csv = os.path.dirname(os.path.abspath(__file__)) + f'/output/{dim_col}_{metric_file}_yearly_output.csv'
        with open(output_csv, "w") as file:
            header = year_col + [dim_col] + ['sum'] + ['count'] + ['avg']
            file.write(",".join(header) + "\n")
            for i, row in data_file.drop_duplicates(subset=dim_col).iterrows():
                data_row = []
                for col in header:
                    if col in year_col:
                        data_row.append(row[col])
                    elif col == dim_col:
                        data_row.append(row[dim_col])
                file.write(",".join([str(x) for x in data_row]) + "\n")
        print(f"Output file {output_csv} created successfully.")
    return True

def monthly(dim_list,data_file, metric_file):
    data_file = pd.read_csv(data_file)
    month_col = [data_file.columns[-2], data_file.columns[-1]]

    for dim_col in dim_list:
        output_csv = os.path.dirname(os.path.abspath(__file__)) + f'/output/{dim_col}_{metric_file}_monthly_output.csv'
        with open(output_csv, "w") as file:
            header = month_col + [dim_col] + ['sum'] + ['count'] + ['avg']
            file.write(",".join(header) + "\n")
            for i, row in data_file.drop_duplicates(subset=dim_col).iterrows():
                data_row = []
                for col in header:
                   if col in month_col:
                        data_row.append(row[col])
                   elif col == dim_col:
                        data_row.append(row[dim_col])
                file.write(",".join([str(x) for x in data_row]) + "\n")
        print(f"Output file {output_csv} created successfully.")

def weekly(dim_list, file, metric_file):
    data_file = pd.read_csv(file)
    week_col = [data_file.columns[-3], data_file.columns[-1]]
    for dim_col in dim_list:
        output_csv = os.path.dirname(os.path.abspath(__file__)) + f'/output/{dim_col}_{metric_file}_weekly_output.csv'
        with open(output_csv, "w") as file:
            header = week_col + [dim_col] + ['sum'] + ['count'] + ['avg']
            file.write(",".join(header) + "\n")
            for i, row in data_file.drop_duplicates(subset=dim_col).iterrows():
                data_row = []
                for col in header:
                    if col in week_col:
                        data_row.append(row[col])
                    elif col == dim_col:
                        data_row.append(row[dim_col])
                file.write(",".join([str(x) for x in data_row]) + "\n")
        print(f"Output file {output_csv} created successfully.")

def generate_table(program_name, num_rows):
    json_file = os.path.dirname(os.path.abspath(__file__)) + f'/input/{program_name}_config.json'
    with open(json_file) as f:
        data = json.load(f)
    dimensions = data[0]['input']['dimensions']
    metrics = data[0]['input']['metrics']
    rows = []

    # Generate a list of values for each dimension
    dim_values = {}
    for dim in dimensions:
        dim_values[dim] = dimensions[dim] if isinstance(dimensions[dim], list) else [dimensions[dim]]

    # Generate a list of metrics and their values
    metric_values = {}
    for metric in metrics:
        metric_values[metric] = list(range(metrics[metric]['min'], metrics[metric]['max'] + 1))

    # Iterate through each metric and generate rows for each combination of dimension and metric values
    for metric in metrics:
        metric_rows = []
        for i in range(num_rows):
            row = {}
            for dim in dim_values:
                row[dim] = dim_values[dim][i % len(dim_values[dim])]
            row[metric] = random.choice(metric_values[metric])

            # Extract week, month, and year from the 'date' dimension and add them to the row
            date_string = row['date']
            date_obj = datetime.strptime(date_string, '%Y-%m-%d')
            row['week'] = date_obj.isocalendar()[1]
            row['month'] = date_obj.month
            row['year'] = date_obj.year
            metric_rows.append(row)

        # Create a pandas dataframe from the rows for this metric and print it
        df = pd.DataFrame(metric_rows)
        df = df.drop_duplicates(subset=list(dimensions.keys()))
        df.to_csv(os.path.dirname(os.path.abspath(__file__))+f'/output/output_dummy_{program_name}_{metric}.csv', index=False)

    return f"Successfully generated the dummy data for {program_name}"
