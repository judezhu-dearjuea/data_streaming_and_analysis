#!/bin/bash
#set limits
MAXUSER=9
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
    STAGE=$(((RANDOM % 9)))
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
    #if No. of user falls below two, generate user and enemy. Also applies to the case of initial requests
    #if [[ $NOOFUSER < 2 ]]; then
    #    echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER}stage 0, less than 2 players, create two players"
    #    let NOOFUSER=NOOFUSER+2
    #fi
    case $STAGE in
        0)  #add enemy
            #echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} add an enemy"
            docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" http://localhost:5000/initialize?username=$ENEMYID
            let NOOFUSER=NOOFUSER+1
            ;;
        1|2)  #purchase weapon
            WEAPONPURCHASE=${WEAPON[$(($RANDOM % ${#WEAPON[@]}))]}
            #echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} trying to purchase weapon ${WEAPONPURCHASE}"
            docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" "http://localhost:5000/purchase_weapon?username=$USERID&weapon=$WEAPONPURCHASE"
            ;;
        3)  #purchase shield
            #echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} trying to purchase shield"
            docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" http://localhost:5000/purchase_shield?username=$USERID
            ;;
        4|5|6|7)  #attack enemy
            #echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} trying to attack with ${WEAPONATTACK}"
            docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" "http://localhost:5000/attack?username=$USERID&enemy=$ENEMYID"
            ;;
        8)  #dig for gold
            #echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} dig gold"
            docker-compose exec mids ab -n 5 -H "Host: user1.comcast.com" http://localhost:5000/dig_for_gold?username=$USERID
            ;;
    esac
    let REQS=REQS+1
done
