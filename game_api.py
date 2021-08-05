#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request
import redis
from numpy import random

app = Flask(__name__)

producer = KafkaProducer(bootstrap_servers='kafka:29092')

redis_store = redis.Redis(
    host='redis',
    port='6379'
)

weapon_prices = {
    "Stick": 0,
    "Knife": 5,
    "Sword": 10,
    "Grenade": 15,
    "Gun": 50,
    "Bazooka": 100,
    "Nuke": 500
}
weapon_damage = {
    "Stick": 0,
    "Knife": 2,
    "Sword": 3,
    "Grenade": 5,
    "Gun": 9,
    "Bazooka": 20,
    "Nuke": 100
}
weapon_success_rate = {
    "Stick": 1,
    "Knife": 0.7,
    "Sword": 0.65,
    "Grenade": 0.4,
    "Gun": 0.75,
    "Bazooka": 0.5,
    "Nuke": 1
}

shield_price = 20
shield_protection = 5

shovel_price = 5
#25 gold scaling to make it interesting
#original 5
gold_scaling = 25

def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())

@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"

# Call to initialize a game
@app.route("/initialize",methods = ['GET','POST'])
def initialize():
    user_name = request.args.get('username',default='player',type=str)
    
    if redis_store.hexists('user_alive',user_name):
        return "Username Taken! Please Input Another Username. \n"
    
    else:
        wallet = 100
        health = 20
        weapon = 'Stick'
        shield = 0
        redis_store.hset('user_alive',user_name, 1)
        redis_store.hset('wallet',user_name, wallet)
        redis_store.hset('health',user_name, health)
        redis_store.hset('weapon',user_name, weapon)
        redis_store.hset('shield',user_name, shield)
        initial_event = {
            'event_type':'initiaize player',
            'username':user_name
        }
        log_to_kafka('events',initial_event)
        
        return (
            "Game Started!\nYou have %i in wallet, %i in health and a %s as a weapon. \n" % (wallet, health, weapon)
        )
    
# Call to purchase a weapon  
@app.route("/purchase_weapon",methods = ['GET','POST'])
def purchase_weapon():
    user_name = request.args.get('username',type=str)
    weapon = request.args.get('weapon',default='Stick',type=str)
    
    if not redis_store.hexists('user_alive',user_name):
        return "Not Valid Username! Please Initialize.\n"
    elif int(redis_store.hget('user_alive',user_name)) == 0:
        return "User is Dead! Please Start a New Game. \n"
    elif weapon not in weapon_prices.keys():
        return_string = ", ".join(str(x) for x in weapon_prices.keys())
        return "Invalid Weapon! Please Input a Valid  Weapon from below.\n%s\n" % return_string
    elif int(redis_store.hget('wallet',user_name)) < weapon_prices[weapon]:
        return "Not enough money in wallet! Get more to purchase weapon.\n"
    else:
        weapon_price = weapon_prices[weapon]
        wallet_before = int(redis_store.hget('wallet', user_name))
        wallet_after = wallet_before - weapon_price
        redis_store.hset('wallet', user_name, wallet_after)
        redis_store.hset('weapon', user_name, weapon)
        purchase_weapon_event = {
            'event_type': 'purchase_weapon',
            'username': user_name,
            'weapon': weapon,
            'wallet_before': wallet_before,
            'wallet_after': wallet_after
        }
        log_to_kafka('events', purchase_weapon_event)
        return "%s Purchased!\n" % weapon

# Call to purchase a shield
@app.route("/purchase_shield",methods = ['GET','POST'])
def purchase_shield():
    user_name = request.args.get('username',type=str)
    if not redis_store.hexists('user_alive',user_name):
        return "Not Valid Username! Please Initialize.\n"
    elif int(redis_store.hget('user_alive',user_name)) == 0:
        return "User is Dead! Please Start a New Game. \n"
    elif int(redis_store.hget('wallet',user_name)) < shield_price:
        return "Not enough money in wallet! Get more to purchase a shield.\n"
    else:
        wallet_before = int(redis_store.hget('wallet', user_name))
        wallet_after = wallet_before - shield_price
        redis_store.hset('wallet', user_name, wallet_after)
        redis_store.hset('shield', user_name, 1)
        purchase_shield_event = {
            'event_type': 'purchase_shield',
            'username': user_name,
            'wallet_before': wallet_before,
            'wallet_after': wallet_after
        }
        log_to_kafka('events', purchase_shield_event)
        return "shield Purchased!\n"

# Call to dig for gold
@app.route("/dig_for_gold",methods = ['GET','POST'])
def dig_for_gold():
    user_name = request.args.get('username',type=str)
    if not redis_store.hexists('user_alive',user_name):
        return "Not Valid Username! Please Initialize.\n"
    elif int(redis_store.hget('user_alive',user_name)) == 0:
        return "User is Dead! Please Start a New Game. \n"
    elif int(redis_store.hget('wallet',user_name)) < shovel_price:
        return "Not enough money in wallet! Get more to dig for gold.\n"
    else:    
        wallet_before = int(redis_store.hget('wallet', user_name))
        gold_found = int(random.exponential(gold_scaling))
        wallet_after = wallet_before - shovel_price + gold_found
        redis_store.hset('wallet', user_name, wallet_after)
        dig_for_gold_event = {
            'event_type': 'dig_for_gold',
            'username': user_name,
            'gold_found': gold_found,
            'wallet_before': wallet_before,
            'wallet_after': wallet_after
        }
        log_to_kafka('events', dig_for_gold_event)
        return "Found %i in Gold!\n" % gold_found

    
# Call to attack a player
@app.route("/attack",methods=['GET','POST'])
def attack():
    user_name = request.args.get('username',type=str)
    enemy = request.args.get('enemy',type=str)
    if not redis_store.hexists('user_alive',user_name):
        return "Not Valid Username! Please Initialize.\n"
    if not redis_store.hexists('user_alive',enemy):
        return "Not Valid Enemy Username! Please Attack a Valid Enemy.\n"
    elif int(redis_store.hget('user_alive',user_name)) == 0:
        return "User is Dead! Please Start a New Game. \n"
    elif int(redis_store.hget('user_alive',enemy)) == 0:
        return "Enemy is Dead! Don't kick a man when they're down. \n"
    else:
        weapon = redis_store.hget('weapon',user_name)
        enemy_shield = bool(int(redis_store.hget('shield',enemy)))
        enemy_health_before = int(redis_store.hget('health',enemy))
        attack_successful = bool(random.binomial(1, weapon_success_rate[weapon]))
        if attack_successful:
            enemy_health_after = None
            if enemy_shield:
                damage = max(0, weapon_damage[weapon] - shield_protection)
                enemy_health_after = max(0, enemy_health_before - damage)   
            else:
                damage = weapon_damage[weapon]
                enemy_health_after = max(0, enemy_health_before - damage)
            
            enemy_killed = (enemy_health_after == 0)
            redis_store.hset('health', enemy, enemy_health_after)
            
            successful_attack_event = {
                'event_type': 'successful_attack',
                'attacker': user_name,
                'defender': enemy,
                'weapon_used': weapon,
                'defender_has_shield' : enemy_shield,
                'defender_health_before': enemy_health_before,
                'defender_health_after': enemy_health_after,
                'defender_killed': enemy_killed
            }
            log_to_kafka('events',successful_attack_event)
            
            if enemy_killed:
                wallet = int(redis_store.hget('wallet', user_name))
                money_won = int(redis_store.hget('wallet', enemy))
                redis_store.hset('user_alive', enemy, 0)
                redis_store.hset('wallet', enemy, 0)
                redis_store.hset('wallet', user_name, wallet+money_won)
                return "Killed Enemy with %s!\n You won $%i!\n" % (weapon, money_won)
            else:
                return "Attacked Enemy with %s! Enemy has %i Health Left.\n" % (weapon, enemy_health_after)
        else:
            failed_attack_event = {
                'event_type': 'failed_attack',
                'attacker': user_name,
                'defender': enemy,
                'weapon_used': weapon
            }
            log_to_kafka('events', failed_attack_event)
            return "Attacked with %s Failed! \n" % (weapon)
