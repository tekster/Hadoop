#/bin/bash
OWNER=$(/local/presto-cli/presto --server a9-sl-sldsp-presto.iad.amazon.com:8080 --catalog hive --schema default --execute "select distinct username from HDFS_META")
echo $OWNER

for i in ${OWNER}
do
OWN=`echo $i| tr -d '"'`
echo "extracting data for $OWN"
/local/presto-cli/presto --server a9-sl-sldsp-presto.iad.amazon.com:8080 --catalog hive --schema default --output-format TSV --execute "with
t1 as (select TBLS.OWNER HIVE_OWNER,DBS.NAME DB_NAME, TBLS.TBL_NAME, from_unixtime(TBLS.CREATE_TIME) TBL_CREATE_TIME ,
       substr(SDS.LOCATION,30) PATH,PARTITIONS.PART_NAME PARTITION_NAME,from_unixtime(PARTITIONS.CREATE_TIME) PARTITION_CREATE_TIME
       from TBLS, DBS, SDS, PARTITIONS
       where DBS.DB_ID=TBLS.DB_ID and TBLS.TBL_ID=PARTITIONS.TBL_ID and PARTITIONS.SD_ID=SDS.SD_ID
       and TBLS.OWNER='${OWN}'
       union
       select TBLS.OWNER HIVE_OWNER,DBS.NAME DB_NAME, TBLS.TBL_NAME, from_unixtime(TBLS.CREATE_TIME) TBL_CREATE_TIME ,
       substr(SDS.LOCATION,30) PATH,PARTITIONS.PART_NAME PARTITION_NAME, from_unixtime(PARTITIONS.CREATE_TIME) PARTITION_CREATE_TIME
       from TBLS
       join DBS on DBS.DB_ID=TBLS.DB_ID join SDS on SDS.SD_ID=TBLS.SD_ID
       left outer join PARTITIONS on TBLS.TBL_ID=PARTITIONS.TBL_ID
       where TBLS.OWNER='${OWN}'
 ),
t2 as (select username HDFS_OWNER,path,modification_time HDFS_CREATE_TIME,permission,accesstime
       from HDFS_META
       where username='${OWN}')
SELECT '3p hadoop fs -rm ' || t2.path
FROM t2
LEFT OUTER  JOIN t1 ON t2.path=t1.path
where t2.HDFS_CREATE_TIME  < (current_date - interval '13' month)
and   substr(t2.permission,1,1) <> 'd'
order by t1.HIVE_OWNER,t1.DB_NAME,t1.TBL_NAME;" >/local/rescamil/rm_old_${OWN}.sh
done
