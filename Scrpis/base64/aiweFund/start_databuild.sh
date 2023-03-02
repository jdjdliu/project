docker run -it \
    -u root \
    -v /data/cmb_bigquant/var/app/data/bigquant:/var/app/data/bigquant \
    -e DATAPLATFORM_URL="http://aiwe-quantplatform-data.paas.cmbchina.cn" \
    csapdev.registry.cmbchina.cn/lx58/aiwequantplatformuserbox:xxxxxx \
    bash