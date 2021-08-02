#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request
import redis

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')
redis_store = redis.Redis(
    host='redis',
    port='6379')

weapon_prices = {
    "stick": 0,
    "knife": 5,
    "sword": 10,
    "grenade": 15,
    "gun": 50,
    "bazooka": 100,
    "nuke": 500
}

def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())

@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"

@app.route("/initialize",methods = ['GET','POST'])
def initialize():
    user_name = request.args.get('username',default='player',type=str)
    
    if redis_store.hexists("wallet",user_name):
        return "Username Taken! Please Input Another Username. \n"
    
    else:
        wallet = 100
        health = 10
        weapon = "stick"
        redis_store.hset("wallet",user_name, wallet)
        redis_store.hset("health",user_name, health)
        redis_store.hset("weapon",user_name, weapon)
        initial_event = {'event_type':'initiaize player',
                         'username':user_name}
        log_to_kafka('events',initial_event)
        
        return (
            "Game Started!\nYou have %i in wallet, %i in health and a %s as a weapon. \n" % (wallet, health, weapon)
        )
    
#docker-compose exec mids curl "http://localhost:5000/purchase_weapon?w=sword&n=3"    
@app.route("/purchase_weapon",methods = ['GET','POST'])
def purchase_weapon():
    user_name = request.args.get('username',type=str)
    weapon = request.args.get('weapon',default='stick',type=str)
    
    if not redis_store.hexists("wallet",user_name):
        return "Not Valid Username! Please Initialize.\n"
    elif weapon not in weapon_prices.keys():
        return_string = ", ".join(str(x) for x in weapon_prices.keys())
        return "Invalid Weapon! Please Input a Valid  Weapon from below.\n" + return_string + "\n"
    elif int(redis_store.hget("wallet",user_name)) < weapon_prices[weapon]:
        return "Not enough money in wallet! Get more to purchase weapon.\n"
    else:
        weapon_price = weapon_prices[weapon]
        wallet_before = int(redis_store.hget("wallet", user_name))
        wallet_after = wallet_before - weapon_price
        redis_store.hset("wallet", user_name, wallet_after)
        redis_store.hset("weapon", user_name, weapon)
        purchase_weapon_event = {
            'event_type': 'purchase_weapon',
            'weapon': weapon,
            'wallet_before': wallet_before,
            'wallet_after': wallet_after
        }
        log_to_kafka('events', purchase_weapon_event)
        return weapon + " Purchased!\n"


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