#!/bin/bash

while true
do
	x=$(free -m|grep Mem|awk '{print $7}')
	y=768
	#x=$(free -m|grep Mem|awk '{print $7/$2}')
	#echo $x
	if [ $x -lt $y ]
	then
		cd /usr/share/sounds/alsa/
		aplay Front_Center.wav -q
	else
		sleep 10
		
	fi
done
