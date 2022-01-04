from flask import Flask, render_template,request,jsonify
import dbmongo
import pprint
import json
from bson.json_util import ObjectId,loads,dumps
from sshtunnel import SSHTunnelForwarder
from ssh_pymongo import MongoSession


class MyEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super(MyEncoder, self).default(obj)

app = Flask(__name__)
app.json_encoder = MyEncoder

db = dbmongo.db_import()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/page1',methods=['GET','POST'])
def page1():
    if request.method == "POST":
        name = request.form.get("institution_name")
        print(name)
        award_id = int(request.form.get("award_id"))
        award_title = request.form.get("award_title")
        query = dbmongo.query1(name,award_title,award_id,db)
        results = query[0]['awards']['investigators']
        return render_template('page1_results.html',results=results,id=award_id,title=award_title)
    return render_template('page1.html')


@app.route('/page2',methods=['GET','POST'])
def page2():
    if request.method == "POST":
        name = request.form.get("institution_name")
        query = dbmongo.query2(name,db)
        results = query[0]['awards']
        print(results)
        return render_template('page2_results.html',results=results,name=name)
    return render_template('page2.html')

@app.route('/page4',methods=['GET','POST'])
def page4():
    if request.method == "POST":
        first_name = request.form.get("first_name")
        last_name = request.form.get("last_name")
        email_id = request.form.get("email_id")
        query = dbmongo.query4(first_name,last_name,email_id,db)
        results = query
        print(results)
        return render_template('page4_results.html',results=results,first_name=first_name,last_name=last_name)
    return render_template('page4.html')

@app.route('/page3',methods=['GET','POST'])
def page3():
    if request.method == "POST":
        name = request.form.get("scientific domain")
        query = dbmongo.query3(name,db)
        results = query[0]['w_award']
        return render_template('page3_results.html',results=results,name=name)
    return render_template('page3.html')

@app.route('/page5',methods=['GET','POST'])
def page5():
    query = dbmongo.query5(db)
    results = query
    return render_template('page5.html',results=results)


@app.route('/page7',methods=['GET','POST'])
def page7():
    if request.method == "POST":
        date1 = request.form.get("date1")
        date2 = request.form.get("date2")
        query = dbmongo.query7(date1,date2,db)
        results = query
        results2 = query[0]['awards']
        return render_template('page7_results.html',results=results,results2=results2,date1=date1,date2=date2)
    return render_template('page7.html')

@app.route('/page6',methods=['GET','POST'])
def page6():
    if request.method == "POST":
        date1 = request.form.get("date1")
        date2 = request.form.get("date2")
        query = dbmongo.query6(date1,date2,db)
        results = query
        return render_template('page6_results.html',results=results,date1=date1,date2=date2)
    return render_template('page6.html')

@app.route('/page8',methods=['GET','POST'])
def page8():
    query = dbmongo.query8(db)
    results = query
    return render_template('page8.html',results=results)