#!/bin/bash
DATAPLATFORM_URL="http://aiwe-quantplatform-api-data.paas.cmbchina.cn"
DOCKER_IMAGE="csapprd.registry.cmbchina.cn/lx58/aiwequantplatformdataproxy:cs_lx58_aiwequantplatformdataproxy_master_uat_20220418_02"
OUT_LOG_PATH="/mnt/var/app/data/bigquant/datasource/data_build/update_logs/"
LOG_PATH="/var/app/data/bigquant/datasource/data_build/update_logs/"
LOG_FILE=${LOG_PATH}`date +%Y%m%d`".txt"
OUT_LOG_FILE=${OUT_LOG_PATH}`date +%Y%m%d`".txt"
if [ ! -e $OUT_LOG_PATH ]
then
  mkdir $OUT_LOG_PATH
  echo "finish mkdir: ${OUT_LOG_PATH}"
fi
BASE_SCRIPT_PATH="/var/app/data/bigquant/datasource/data_build/aiweFund/"


build_data_scripts=(
  'aiflow_dags/builddata_by_shell.py'
)


base_cmd="docker run --rm -u root -v /mnt/var/app/data/bigquant:/var/app/data/bigquant -e DATAPLATFORM_URL=${DATAPLATFORM_URL} ${DOCKER_IMAGE} "

echo ">>> [`date +'%Y%m%d-%H:%M:%S'`] start build data ...."

build_data_sc='aiflow_dags/builddata_by_shell.py'
cmd="${base_cmd} bash -c -e -v 'python3 ${BASE_SCRIPT_PATH}${build_data_sc} >> ${LOG_FILE}' "
echo -e "${i} start execute cmd [`date +'%Y%m%d-%H:%M:%S'`]:\n     ${cmd} \n" >> ${OUT_LOG_FILE}
bash -c "${cmd}"
echo -e "${i} end execute cmd [`date +'%Y%m%d-%H:%M:%S'`]:\n     ${cmd} \n" >> ${OUT_LOG_FILE}
