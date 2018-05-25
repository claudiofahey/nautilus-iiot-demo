#!/usr/bin/env python

import sys
import json
import flask
import os
import itertools
import pickle
import pandas as pd
import numpy as np
import sklearn
from six import StringIO
from flask import g, render_template, Response

app = flask.Flask(__name__)
app.secret_key = os.urandom(24)

@app.route("/predict", methods=['POST'])
def predict():
    input_json = flask.request.data
    print('input_json=%s' % str(input_json))
    input_json = '[' + input_json + ']'
    input_df = pd.read_json(StringIO(input_json))
    input_df.info()
    result_df = input_df
    result_df['new_timestamp'] = result_df['timestamp'] * 1000
    result_json = input_df.to_json(orient='records')
    return Response(result_json, mimetype='application/json')

if __name__ == "__main__":
    print('prediction_web_service.py: starting')
    app.run(host='0.0.0.0', debug=True, port=5001)
