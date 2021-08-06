#!/bin/bash
#set limits
MAXUSER=50
#MINUSER=0
MAXREQ=1000
#initialize incrementer
#NOOFUSER=0
REQS=0
#weapon inventory array
WEAPON=("Stick" "Knife" "Sword" "Grenade" "Gun" "Bazooka" "Nuke")

#whileloop to generate requests until max request is met or max user reached
while [[ $REQS -le $MAXREQ ]]; do
    ID1=$(( ( RANDOM % MAXUSER) + 1 ))
    USERID="user${ID1}"
    ID2=$(( ( RANDOM % MAXUSER) + 1 ))
    ENEMYID="user${ID2}"
    #STAGE randomizer
    STAGE=$(( RANDOM % 9 ))
    #if USERID and ENEMYID are the same, regenerate ENEMYID
    while [[ $USERID == $ENEMYID ]]; do
        TEMPID=$(( ( RANDOM % MAXUSER ) + 1 ))
        ENEMYID="user${TEMPID}"
    done
    #initialize on 1st request
    if [[ $REQS == 1 ]]; then
        docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" http://localhost:5000/initialize?username=$USERID
        docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" http://localhost:5000/initialize?username=$ENEMYID
    fi

    case $STAGE in
        0)  #add enemy
            docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" http://localhost:5000/initialize?username=$ENEMYID
            ;;
        1|2)  #purchase weapon
            WEAPONPURCHASE=${WEAPON[$(($RANDOM % ${#WEAPON[@]}))]}
            docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" "http://localhost:5000/purchase_weapon?username=$USERID&weapon=$WEAPONPURCHASE"
            ;;
        3)  #purchase shield
            docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" http://localhost:5000/purchase_shield?username=$USERID
            ;;
        4|5|6|7)  #attack enemy
            docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" "http://localhost:5000/attack?username=$USERID&enemy=$ENEMYID"
            ;;
        8)  #dig for gold
            docker-compose exec mids ab -n 5 -H "Host: user1.comcast.com" http://localhost:5000/dig_for_gold?username=$USERID
            ;;
    esac
    let REQS=REQS+1
done
