#!/bin/sh

day=$1
DATE=`date +%Y%m%d -d "-$(($day+22)) day"`
DEAL_DATE=`date +%Y%m%d -d "-$(($day+122)) day"`
PT=${DATE}000000
echo $PT
LAST_PT=${DEAL_DATE}000000
echo $LAST_PT
HOUSE_HOT=house_hot_base.dat

hive -e "use data_center; select * from hot_bd_dw_house_showing_num_day where pt <=\"${PT}\" and pt > \"${LAST_PT}\"; " > ./data/${HOUSE_HOT}

