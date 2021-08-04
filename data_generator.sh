#!/bin/bash
MAXUSER=10
MINUSER=0
NOOFUSER=0
REQS=0
MAXREQ=10
WEAPON=("Stick" "Knife" "Sword" "Grenade" "Gun" "Bazooka" "Nuke")
IDCHECK=false
#idcheck() {
 #   if [[ $USERID == $ENEMYID ]]; then
  #      return true
   # else
    #    return false
    #fi

#}
#until [[$NOOFUSER -le $MINUSER] || [$NOOFUSER -gt $MAXUSER] || [$REQS -gt $MAXREQ]; do
while [[ $NOOFUSER -le $MAXUSER && $REQS -le $MAXREQ ]]; do
    ID=$(( ( RANDOM % MAXUSER) + 1 ))
    USERID="user${ID}"
    ID2=$(( ( RANDOM % MAXUSER) + 1 ))
    ENEMYID="user${ID2}"
    STAGE=$(((RANDOM % 5)))
    if [[ $USERID == $ENEMYID ]]; then
        ID3=$(( ( RANDOM % MAXUSER ) + 1 ))
        ENEMYID="user${ID3}"
    fi
    #while $IDCHECK; do
     #   
      #  break
    #done
    if [[ $NOOFUSER < 2 ]]; then
        echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER}stage 0, less than 2 players, create two players"
        #docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/initialize?username=USERID
        #docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/initialize=ENEMYID
        let NOOFUSER=NOOFUSER+2
    fi
    case $STAGE in
        0)  echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} add an enemy"
            #docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/initialize?username=ENEMYID
            let NOOFUSER=NOOFUSER+1
            ;;
        1)  
            WEAPONPURCHASE=${WEAPON[$(($RANDOM % ${#WEAPON[@]}))]}
            echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} trying to purchase weapon ${WEAPONPURCHASE}"
            #docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/purchase_weapon?username=USERID&weapon=WEAPONPURCHASE
            ;;
        2)  
            echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} trying to purchase shield"
            #docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/purchase_shield?username=USERID
            ;;
        3)  
            WEAPONATTACK=${WEAPON[$(($RANDOM % ${#WEAPON[@]}))]}
            echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} trying to attack ${WEAPONATTACK}"
            #RESULT=docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/attack username=USERID&weapon=WEAPONATTACK
            RESULT="attack successful"
            if [[ $RESULT =~ "success" ]]; then
                let NOOFUSER=NOOFUSER-1
            fi
            ;;
        4)
            echo "Stagge ${Stage} REQS ${REQS} USER ${NOOFUSER} dig gold"
            #docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/dig_for_gold?username=USERID
            ;;
    esac
    let REQS=REQS+1
done