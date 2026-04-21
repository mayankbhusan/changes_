
# Pre-build parent metadata lookup
# Format: table_file_id|country|hdfs_nas_path|source|tp
PARENT_LOOKUP="/tmp/parent_lookup_${env}.tmp"
awk -F'|' 'NR>1 {print $1"|"$3"|"$6"|"$2"|"$4}' \
    ${PARENT_META} > ${PARENT_LOOKUP}
loginfo "Parent lookup ready: $(wc -l < ${PARENT_LOOKUP}) entries"


------

# Lookup country and hdfs_nas_path from parent metadata
parent_row=$(grep "^${table_file_id}|" ${PARENT_LOOKUP} | head -1)
country=$(echo "${parent_row}" | cut -d'|' -f2)
hdfs_nas_path=$(echo "${parent_row}" | cut -d'|' -f3)
source=$(echo "${parent_row}" | cut -d'|' -f4)
tp=$(echo "${parent_row}" | cut -d'|' -f5)


_____
cobs_size_gb=$(grep "^${table_file_id}|" "${COBS_LOOKUP}" \
    | head -1 | cut -d'|' -f2)

------
# Use hdfs_nas_path from parent metadata directly
raw_bytes=$(grep -iF "${hdfs_nas_path}" \
    "${HDFS_SIZE_LOOKUP}" | head -1 | awk '{print $1}')

-----
echo "${table_file_id}|${country}|${bucket}|${size_gb}|${run_date}" >> ${TEMP_CFG}


-----
echo "${table_file_id}|${source_table_name}|${brnz_schema}|\
${brnz_table}|${hdfs_nas_path}|${source}|${tp}|\
${size_gb}|${bucket}|${country}|${run_date}|${run_ts}" >> /tmp/registry_data.csv


-----


