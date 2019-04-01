#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from flask import Flask, jsonify, request
from multiprocessing import Value
from google.cloud import bigquery

app = Flask(__name__)

help_message = """
API Usage:
    POST /donorschoose/projects/new data={
            'school_id': school_id,
            'teacher_id': teacher_id,
            'teacher_proj_posted_seq': teacher_proj_posted_seq,
            'proj_type': proj_type,
            'proj_title': proj_title,
            'proj_essay': proj_essay,
            'proj_short_description': proj_short_description,
            'proj_need_stat': proj_need_stat,
            'proj_category': proj_category,
            'proj_sub_categ': proj_sub_categ,
            'proj_grade_level': proj_grade_level,
            'proj_resource': proj_resource,
            'proj_cost': proj_cost,
            'proj_exp_date': proj_exp_date}
    POST /donorschoose/donations/new data={
            'donor_id': donor_id,
            'donor_opt': donor_opt,
            'amount': amount,
            'cart_seq': cart_seq
    }
    GET /donorschoose/projects/findByDonor/?donor=v1
    GET /donorschoose/projects/findByStatus/?status=v1
    GET /donorschoose/projects/findByNeed/?state=v1
"""
"""    
   exemplo abaixo 
- GET    /api/list
- POST   /api/add data={"key": "value"}
- GET    /api/get/<id>
- PUT    /api/update/<id> data={"key": "value_to_replace"}
- DELETE /api/delete/<id>

"""
bigclient=bigquery.Client()
dataset_id = 'handy-zephyr-235119.donorschoose'
dataset_ref = bigclient.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)

@app.route('/donorschoose/projects/new', methods=['POST'])
def new_project():
    payload = request.json
    #query the database
    return help_message

@app.route('/donorschoose/donations/new', methods=['POST'])
def new_donation():
    payload = request.json
     #query the database
     #return the results with jsonify(result)
    return jsonify(a)

#Projetos
@app.route('/donorschoose/projects/findByDonor', methods=['GET'])
def find_by_donor():
    donor = request.args.get('donor', None)
    projects_table='`'+dataset_id+'.Projects`'
    donations_table='`'+dataset_id+'.Donations`'
    query=('SELECT Project_Title FROM ' + projects_table + ' WHERE Project_ID IN (SELECT Project_ID FROM '+ donations_table+' WHERE Donor_ID="'+donor+'") LIMIT 100')
    query_job = bigclient.query(query)
    rows = query_job.result()
    answer = []
    for row in rows:
        answer.append(row.Project_Title)
    return jsonify(answer)

@app.route('/donorschoose/projects/findByStatus', methods=['GET'])
def find_by_status():
    status = request.args.get('status', None)
    projects_table='`'+dataset_id+'.Projects`'
    query=('SELECT Project_Title FROM ' + projects_table + ' WHERE Project_Current_Status="'+status+'" LIMIT 100')
    query_job = bigclient.query(query)
    rows = query_job.result()
    answer = []
    for row in rows:
        answer.append(row.Project_Title)
    return jsonify(answer)

@app.route('/donorschoose/projects/findByNeed', methods=['GET'])
def find_by_need():
    state = request.args.get('state', None)
     #query the database
    return jsonify(a)

if __name__ == '__main__':
    app.run()
'''
@app.route('/donorschoose/add', methods=['POST'])
def index():
    payload = request.json
    a.append(payload)
    return "Created: {} \n".format(payload)

@app.route('/api/get', methods=['GET'])
def get_none():
    return 'ID Required: /api/get/<id> \n'

@app.route('/api/get/<int:_id>', methods=['GET'])
def get(_id):
    for user in a:
        if _id == user['id']:
            selected_user = user
    return jsonify(selected_user)

@app.route('/api/update', methods=['PUT'])
def update_none():
    return 'ID and Desired K/V in Payload required: /api/update/<id> -d \'{"name": "john"}\' \n'

@app.route('/api/update/<int:_id>', methods=['PUT'])
def update(_id):
    update_req = request.json
    key_to_update = update_req.keys()[0]
    update_val = (item for item in a if item['id'] == _id).next()[key_to_update] = update_req.values()[0]
    update_resp = (item for item in a if item['id'] == _id).next()
    return "Updated: {} \n".format(update_resp)

@app.route('/api/delete/<int:_id>', methods=['DELETE'])
def delete(_id):
    deleted_user = (item for item in a if item['id'] == _id).next()
    a.remove(deleted_user)
    return "Deleted: {} \n".format(deleted_user)

if __name__ == '__main__':
    app.run()'''
