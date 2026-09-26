#!/usr/bin/env bash
# Launch a small cluster with the uv bootstrap action, submit whoami_job.py three ways,
# print what each step saw, then delete everything. Run it in CloudShell, which is already signed in:
#   git clone https://github.com/cpsc436c-2026w1/a2-starter.git && cd a2-starter/examples/extra-packages
#   bash test-on-emr.sh m7g.xlarge
set -euo pipefail
cd "$(dirname "$0")"
REG=ca-central-1; ITYPE=${1:-m8g.xlarge}; B=436c-a2-extrapkg-$(date +%s)
AZS=$(aws ec2 describe-instance-type-offerings --region $REG --location-type availability-zone --filters Name=instance-type,Values=$ITYPE --query 'InstanceTypeOfferings[].Location' --output text | tr '\t' ',')
SUBNET=$(aws ec2 describe-subnets --region $REG --filters Name=map-public-ip-on-launch,Values=true Name=availability-zone,Values=$AZS --query 'Subnets[0].SubnetId' --output text)
echo "subnet $SUBNET (an AZ that offers $ITYPE)"
aws s3 mb s3://$B --region $REG >/dev/null
aws s3 cp bootstrap-uv.sh s3://$B/bootstrap-uv.sh --region $REG >/dev/null
aws s3 cp whoami_job.py s3://$B/whoami_job.py --region $REG >/dev/null
aws s3 cp duckdb_in_spark.py s3://$B/duckdb_in_spark.py --region $REG >/dev/null
CID=$(aws emr create-cluster --region $REG --name "a2-extrapkg-$ITYPE" --release-label emr-spark-8.0.0 \
  --applications Name=Spark --ec2-attributes SubnetId=$SUBNET --use-default-roles \
  --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=$ITYPE InstanceGroupType=CORE,InstanceCount=2,InstanceType=$ITYPE \
  --bootstrap-actions Path=s3://$B/bootstrap-uv.sh,Name=uv-env,Args=[3.0.6,1.5.5] \
  --auto-termination-policy IdleTimeout=900 --log-uri s3://$B/logs/ --query ClusterId --output text)
echo "cluster $CID on $ITYPE, bucket $B"
cleanup() { aws emr terminate-clusters --cluster-ids $CID --region $REG || true
  aws emr wait cluster-terminated --cluster-id $CID --region $REG || true; sleep 30
  aws s3 rm s3://$B --recursive --quiet --region $REG; aws s3 rb s3://$B --region $REG >/dev/null; echo "cleaned up"; }
trap cleanup EXIT
aws emr wait cluster-running --cluster-id $CID --region $REG
aws emr describe-cluster --cluster-id $CID --region $REG --query 'Cluster.Status.Timeline.[CreationDateTime,ReadyDateTime]' --output text | sed 's/^/created, ready: /'

step() {  # name, extra spark-submit args, script (default whoami_job.py)
  aws emr add-steps --cluster-id $CID --region $REG --query 'StepIds[0]' --output text \
    --steps "Type=Spark,Name=$1,ActionOnFailure=CONTINUE,Args=[--deploy-mode,client$2,s3://$B/${3:-whoami_job.py}]"
}
S1=$(step default "")
S2=$(step driver-only ",--conf,spark.pyspark.driver.python=/opt/a2-driver-only/bin/python")
VENV=",--conf,spark.pyspark.python=/opt/a2-venv/bin/python,--conf,spark.pyspark.driver.python=/opt/a2-venv/bin/python"
S3=$(step everywhere "$VENV")
S4=$(step duckdb-in-spark "$VENV" duckdb_in_spark.py)
for S in $S1 $S2 $S3 $S4; do aws emr wait step-complete --cluster-id $CID --step-id $S --region $REG 2>/dev/null || true; done
for pair in "default:$S1" "driver-only:$S2" "everywhere:$S3" "duckdb-in-spark:$S4"; do
  n=${pair%%:*}; S=${pair#*:}
  echo; echo "=== $n: $(aws emr describe-step --cluster-id $CID --step-id $S --region $REG --query Step.Status.State --output text)"
  for i in $(seq 1 20); do
    out=$(aws s3 cp s3://$B/logs/$CID/steps/$S/stdout.gz - --region $REG 2>/dev/null | gunzip 2>/dev/null) && [ -n "$out" ] && break; sleep 30; done
  echo "$out" | grep -E "^(DRIVER|EXECUTOR|DUCKDB|SETUP|RESULT|THREADS)" | cut -c1-260
  aws s3 cp s3://$B/logs/$CID/steps/$S/stderr.gz - --region $REG 2>/dev/null | gunzip 2>/dev/null | grep -c "does not yet fully support pandas" | sed "s/^/  pandas-3 warnings from PySpark: /" || true
  aws s3 cp s3://$B/logs/$CID/steps/$S/stderr.gz - --region $REG 2>/dev/null | gunzip 2>/dev/null \
    | grep -E "Exception|Error" | grep -vE "WARN|INFO" | head -3 | cut -c1-260 | sed 's/^/  stderr: /' || true
done
