from flask import Flask, render_template,request,jsonify
import dbmongo
import pprint
import json
from bson.json_util import ObjectId,loads,dumps


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
        #email = results[0]['awards']['investigators'][0]['email_id']
        #first_name = results[0]['awards']['investigators'][0]['first_name']
        #last_name = results[0]['awards']['investigators'][0]['last_name']
        #return "The investigator is: "+first_name+' '+last_name+' , and his mail address is: '+email
        return render_template('page1_results.html',results=results)
    return render_template('page1.html')


@app.route('/page2',methods=['GET','POST'])
def page2():
    if request.method == "POST":
        name = request.form.get("name")
        query = dbmongo.query2(name,db)
        results = query[0]['awards']
        return render_template('page2_results.html',results=results)
    return render_template('page2.html')