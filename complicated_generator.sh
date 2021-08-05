#!/bin/bash
#set limits
MAXUSER=10
MINUSER=0
MAXREQ=20

#initialize incrementer
NOOFUSER=0
REQS=0

#weapon inventory array
WEAPON=("Stick" "Knife" "Sword" "Grenade" "Gun" "Bazooka" "Nuke")

#whileloop to generate requests until max request is met or max user reached
while [[ $NOOFUSER -le $MAXUSER && $REQS -le $MAXREQ ]]; do
    ID1=$(( ( RANDOM % MAXUSER) + 1 ))
    USERID="user${ID1}"
    ID2=$(( ( RANDOM % MAXUSER) + 1 ))
    ENEMYID="user${ID2}"
    
    #STAGE randomizer
    STAGE=$(((RANDOM % 6)))
    
    #if USERID and ENEMYID are the same, regenerate ENEMYID
    while [[ $USERID == $ENEMYID ]]; do
        TEMPID=$(( ( RANDOM % MAXUSER ) + 1 ))
        ENEMYID="user${TEMPID}"
    done
    
    #if No. of user falls below two, generate user and enemy. Also applies to the case of initial requests
    if [[ $NOOFUSER < 2 ]]; then
        echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER}stage 0, less than 2 players, create two players"
        docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/initialize?username=$USERID
        docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/initialize?username=$ENEMYID
        let NOOFUSER=NOOFUSER+2
    fi
    case $STAGE in
        0)  #add enemy
            echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} add an enemy"
            docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/initialize?username=$ENEMYID
            let NOOFUSER=NOOFUSER+1
            ;;
        1)  #purchase weapon
            WEAPONPURCHASE=${WEAPON[$(($RANDOM % ${#WEAPON[@]}))]}
            echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} trying to purchase weapon ${WEAPONPURCHASE}"
            docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" "http://localhost:5000/purchase_weapon?username=$USERID&weapon=$WEAPONPURCHASE"
            ;;
        2)  #purchase shield
            echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} trying to purchase shield"
            docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/purchase_shield?username=$USERID
            ;;
        3)  #attack enemy
            echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} trying to attack with ${WEAPONATTACK}"
            ATTACKRESULT=`docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" "http://localhost:5000/attack?username=$USERID&enemy=$ENEMYID"`
            if [[ $ATTACKRESULT =~ "Killed" ]]; then
                let NOOFUSER=NOOFUSER-1
            fi
            ;;
        4)  #dig for gold
            echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} dig gold"
            docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/dig_for_gold?username=$USERID
            ;;
        5)  #increase weapon purchase frequencies
            WEAPONPURCHASE=${WEAPON[$(($RANDOM % ${#WEAPON[@]}))]}
            echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} trying to purchase weapon ${WEAPONPURCHASE}"
            docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" "http://localhost:5000/purchase_weapon?username=$USERID&weapon=$WEAPONPURCHASE"
            ;;

    esac
    let REQS=REQS+1
done