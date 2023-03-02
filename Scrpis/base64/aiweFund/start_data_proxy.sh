nohup docker run \
    -u root \
    -v /data/cmb_bigquant/var/app/data/bigquant:/var/app/data/bigquant \
    -e DATAPLATFORM_URL="http://aiwe-quantplatform-data.paas.cmbchina.cn" \
    -p 8000:8000 \
    csapdev.registry.cmbchina.cn/lx58/aiwequantplatformdataproxy:xxxxxx \
    >> /data/cmb_bigquant/var/app/data/bigquant/datasource/data_proxy/`date +%Y%m%d`.log &
