import json
import time

from flask import Flask, request, Response
from gen_split_programs import *

app = Flask(__name__)


@app.route('/kpi_test', methods=['POST'])
def kpi_test():
    try:
        program_name = request.json['program_name']
        num_rows = request.json['number_of_rows']
        time_line = request.json['time_period']
        print(time_line,'timeline is ')
        metrics = get_metric(program_name)
        dim_list = dimension_list(program_name)
        time_dimen_list = time_dimension_list(program_name)

        generate_table(program_name, num_rows)
        # time.sleep(5)
        for metric_file in metrics:
            file = (os.path.dirname(
                os.path.abspath(__file__)) + f'/output/output_dummy_{program_name}_{metric_file}.csv')
            print(file, 'is the file')
            if time_line == 'daily':
                daily(dim_list, time_dimen_list, file, metric_file)
            elif time_line == 'yearly':
                yearly(dim_list, file, metric_file)
            elif time_line == 'monthly':
                monthly(dim_list, file, metric_file)
            elif time_line == 'weekly':
                weekly(dim_list, file, metric_file)

        return Response(json.dumps({"Message": "Succesfully generated the files"}))
    except Exception as error:
        print(error)
        return Response(json.dumps({"Message": "Failed to generate the files"}))


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5003)
