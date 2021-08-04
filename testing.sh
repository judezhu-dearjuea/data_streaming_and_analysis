#!/bin/bash

#WEAPON=("Stick" "Knife" "Sword" "Grenade" "Gun" "Bazooka" "Nuke")
#ID1=1
#USERID="user1"


#docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/initialize?username=user1

#WEAPONPURCHASE=${WEAPON[$(($RANDOM % ${#WEAPON[@]}))]}
#echo "trying to purchase weapon ${WEAPONPURCHASE}"
RESULT=${docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" "http://localhost:5000/help"}
echo $RESULT