#!/bin/bash
set -e

FOLDER_ID="b1ghq0b0cq7h3uui6dj1"
NETWORK_ID="enpch1np9ss26887r8tb"
SUBNET_ID="e2ls9japvi3p6onlp9fe"
SG_ID="enpnkb1b5vj89b8q2s34"
BUCKET_NAME="dataproc-bucket-module4-$(date +%s)"

# 1. Create Kafka cluster
echo "Creating Kafka cluster..."
KAFKA_ID=$(yc managed-kafka cluster create \
  --name dataproc-kafka \
  --environment PRODUCTION \
  --network-id $NETWORK_ID \
  --security-group-ids $SG_ID \
  --subnet-ids $SUBNET_ID \
  --zone ru-central1-b \
  --brokers-count 1 \
  --version 3.5 \
  --resource-preset s2.micro \
  --disk-type network-ssd --disk-size 10 \
  --assign-public-ip \
  --format json 2>&1 | grep '"id"' | head -1 | cut -d'"' -f4)
echo "Kafka cluster created: $KAFKA_ID"

# 2. Create Kafka user
echo "Creating Kafka user..."
yc managed-kafka user create \
  --cluster-name dataproc-kafka \
  --name user1 --password password1 \
  --permission topic=*,role=ACCESS_ROLE_ADMIN

# 3. Create Kafka topic
echo "Creating Kafka topic..."
yc managed-kafka topic create \
  --cluster-name dataproc-kafka \
  --name dataproc-kafka-topic \
  --partitions 1 --replication-factor 1

# 4. Create S3 bucket for Data Proc
SA_ID=$(yc iam service-account get spark-s3 --format json | grep '"id"' | head -1 | cut -d'"' -f4)
ACCESS_KEY=$(yc iam access-key create --service-account-id $SA_ID --format json 2>&1)
KEY_ID=$(echo "$ACCESS_KEY" | grep '"key_id"' | cut -d'"' -f4)
SECRET=$(echo "$ACCESS_KEY" | grep '"secret"' | cut -d'"' -f4)

yc storage bucket create --name $BUCKET_NAME
# Grant bucket access
yc storage bucket update --name $BUCKET_NAME --acl grant-read

echo "Bucket $BUCKET_NAME created"

# 5. Create Data Proc cluster
echo "Creating Data Proc cluster (slow, ~10 min)..."
yc dataproc cluster create \
  --name dataproc-cluster \
  --zone ru-central1-b \
  --service-account-id $SA_ID \
  --bucket $BUCKET_NAME \
  --version 2.1 \
  --services HDFS,LIVY,SPARK,YARN,TEZ \
  --subcluster name=main,role=masternode,resource-preset=s2.micro,disk-type=network-hdd,disk-size=20,subnet-id=$SUBNET_ID,hosts-count=1 \
  --subcluster name=data,role=datanode,resource-preset=s2.micro,disk-type=network-hdd,disk-size=20,subnet-id=$SUBNET_ID,hosts-count=1 \
  --ssh-public-key /Users/Lenovo/.ssh/id_rsa_dataproc.pub \
  --ui-proxy true \
  --security-group-ids $SG_ID \
  --async

echo "All resources created!"
echo "Kafka cluster: dataproc-kafka"
echo "Data Proc cluster: dataproc-cluster (creating in background)"
echo "Bucket: $BUCKET_NAME"
echo "Kafka topics: dataproc-kafka-topic"
