#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())

#tracker = {'wallet':100,'enemies':1,'weapon':[]}
@app.route("/initialize")
def initialize():
    initial_event = {'event_type':'initiaize player',
                   'wallet':tracker['wallet'],
                   'enemies':tracker['enemies'],
                   'weapon':tracker['weapon']}
    log_to_kafka('events',initial_event)
    return "Game Started!\n"

    
@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"
    
#@app.route("/purchase_a_sword")
#def purchase_a_sword():
#    purchase_sword_event = {'event_type': 'purchase_sword',
#                            'weapon':'sword',
#                            'cost':20,
#                            'killing_power':0.7}
#    log_to_kafka('events', purchase_sword_event)
#    return "Sword Purchased!\n"
 
#Use this to call - n number of xxx weapon purchased
#docker-compose exec mids curl "http://localhost:5000/purchase_weapon?w=sword&n=3"    
@app.route("/purchase_weapon",methods = ['GET','POST'])
def purchase_weapon():
    weapon_type = request.args.get('w',default='sword',type=str)
    n = request.args.get('n',default=1,type=int)
    purchase_weapon_event = {'event_type': 'purchase_weapon',
                             'weapon_type': weapon_type,
                             'n':n}
    log_to_kafka('events', purchase_weapon_event)
    return str(n) +" " + weapon_type + "(s) Purchased\n"

#Use this to call - n number of players added
#docker-compose exec mids curl "http://localhost:5000/add_player?n=5"    
@app.route("/add_player", methods = ['GET','POST'])
def add_player():
    n = request.args.get('n',default=1,type=int)
    add_player_event = {'event_type':'player_added',
                        'n': n}
    log_to_kafka('events',add_player_event)
    return str(n) + " Player Added!\n"

#Use this to call - kill a player using xxx weapon
#docker-compose exec mids curl "http://localhost:5000/kill_player?w='sword'"    
@app.route("/kill_player",methods=['GET','POST'])
def kill_player():
    weapon_used = request.args.get('w',default='sword',type=str)
#    if tracker['enemies'] >= 1 and weapon_used in tracker['weapon']:
#        tracker['wallet'] += 100
#        tracker['enemies'] -= 1
#        tracker['weapon'].remove(weapon_used)
    kill_player_event = {'event_type':'player_killed',
                         'weapon_used':weapon_used}
    log_to_kafka('events',kill_player_event)
    return "Killed Player with " + str(weapon_used) + "!\n"

@app.route("/add_powerpoints",methods = ['GET','POST'])
def add_powerpoints():
    powerpoints = request.args.get('p',default=0,type=int)
    add_powerpoints_event = {'event_type':'add_powerpoints',
                             'points_added': powerpoints,
                            'points_cost':powerpoints*2}
    log_to_kafka('events',add_powerpoints_event)
    return str(powerpoints) + " Power Points Added for $" + str(powerpoints*2) + " Dollars!"