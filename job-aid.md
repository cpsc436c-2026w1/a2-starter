# A2 job aid: the commands

::: {.callout-warning icon=false title="Draft, watch for updates"}
This is a draft of Assignment 2. The commands, the code and some details may still change
before the release, and the released version is the one that counts.
:::


Every command A2 asks for, in the order it asks. The assignment says why each step is there and
links here for what to type. Nothing in this file tells you what a number should come out as.

Region is `ca-central-1` in every command. Replace the capitalised placeholders first.

| Placeholder | What it stands for |
|---|---|
| `CWL` | your CWL, which goes in the name of every resource |
| `BUCKET` | your bucket, `436c-a2-CWL` |
| `INSTANCE_PROFILE` | `CWL-a2-role`, the instance profile you create under [Once per account](#once-per-account) |
| `SUBNET` | the subnet id you read in [Once per account](#once-per-account). If your account has a default virtual private cloud (VPC, the network your instances are connected to), you can leave `--subnet-id` and `--ec2-attributes SubnetId=` off every command below |
| `GLUE_ROLE` | the account's Glue service role |
| `INSTANCE_ID` | the `i-...` id of the Stage 1 machine |
| `CLUSTER_ID` | the `j-...` id returned by `create-cluster` |
| `STEP_ID` | the `s-...` id of a submitted step |
| `IG_ID` | the `ig-...` id of the cluster's CORE instance group |
| `JOB_RUN_ID` | the `jr_...` id returned by `start-job-run` |
| `LAUNCH_S` | the EMR launch seconds: the create call to the cluster reaching `WAITING` |
| `STARTUP_S` | Glue's start-up seconds, which you compute in Stage 3 |
| `FIT_ROUNDS` | the grid's per-fit round count, published on the book page |

The AWS CLI and your credentials are all these commands need. `uv` is not preinstalled on any of
it. Once per machine:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh && source "$HOME/.local/bin/env"
```

## Where each command runs

Two shells, plus your own laptop. CloudShell is a terminal in the AWS console, in your browser: it
already has the CLI and your credentials, and it runs every command in this file except the Stage 1
job itself. That one runs on the Stage 1 instance, in a shell you open from the EC2 console by
selecting the instance, choosing Connect, and taking the Session Manager tab. Your laptop runs no
AWS command here, and it is where `results.csv` and your screenshots live.

| Commands | Shell | What has to stay running |
|---|---|---|
| [Once per account](#once-per-account), [Get the data](#get-the-data) | CloudShell | the tab, until the fetch loop finishes. CloudShell's home directory survives closing it |
| `run-instances` in [Stage 1 on one EC2 instance](#stage-1-on-one-ec2-instance) | CloudShell | nothing |
| the Stage 1 run itself, and its parse | Session Manager on the Stage 1 instance | the tab, for the length of the run. Anything written to that disk dies when you terminate the instance |
| [Launch the cluster](#launch-the-cluster) through [Stage 3 on Glue](#stage-3-on-glue), and [Shut down and verify](#shut-down-and-verify) | CloudShell | nothing in the shell. The cluster and the Glue job run on AWS whether the tab is open or not |
| `results.csv` and the three screenshots | your laptop | they stay on your laptop from the first row you write to the moment you submit |

## Did the run work?

Four lines to glance at in a log before you parse it. If any of them is wrong, the run is not
comparable to your others, and re-running is the only fix.

- **The block arrived.** `grep -c '===A2-METRICS' YOUR.log` prints 1 for Stage 1 and 2 for Stages 2a, 2b
  and 3. A 0 means the script stopped early, and the reason sits above where the block should
  have been.
- **`mode=rest`** in every block, with `rest_target` naming the host the metrics reader reached. A
  cluster or Glue run that exits 3 reached no Spark REST endpoint and measured nothing. That is a
  failed run, not a slow one.
- **`retried=0`.** A 1 means a stage or task was retried, so the timings are not comparable: rerun.
- **`shuffle_partitions` reads 16**, and `partitions` in the phase A block reads the same 16. If the
  two disagree, the shuffle setting did not take and your two node counts stop being comparable.
  The one run where both read 8 instead is the optional extension below, and it is the only one.
  `tasks` is a different number and is meant to move with the cluster.

## Once per account

All of this runs in CloudShell. EMR will not launch without its service roles.

```bash
aws emr create-default-roles --region ca-central-1
```

A reply saying the roles already exist is a success. Then make the bucket, in the same region.

```bash
aws s3 mb s3://BUCKET --region ca-central-1
```

Check whether your account has a default VPC.

```bash
aws ec2 describe-vpcs --region ca-central-1 \
  --filters Name=is-default,Values=true \
  --query 'Vpcs[0].VpcId' --output text
```

If it prints a `vpc-...` id, your account has a default VPC. You can leave `--subnet-id` and
`--ec2-attributes SubnetId=` off every command below, and skip the next block. If it prints
`None`, run the next command. It prints one subnet id, and you use that same subnet for every
launch in this assignment:

```bash
aws ec2 describe-subnets --region ca-central-1 \
  --filters Name=map-public-ip-on-launch,Values=true \
  --query 'Subnets[0].SubnetId' --output text
```

Save the `subnet-...` value as `SUBNET`. You will pass `--subnet-id SUBNET` to `run-instances`
and `--ec2-attributes SubnetId=SUBNET` to `create-cluster`.

::: {.callout-warning icon=false title="⚠️ If both checks print `None`"}
Your account has no network to launch into. Run `aws ec2 create-default-vpc --region ca-central-1`
once. It creates the default VPC with a public subnet in every zone, and the two checks above then
succeed. If it answers `UnauthorizedOperation`, post on Piazza with your account id and the exact
error. Meanwhile you can do everything up to the launch step (the data fetch and the prediction
do not need an instance).
:::

A2's Stage 1 machine needs its own role, and A1's will not do: A1's is scoped to the a1 bucket and
carries no Session Manager permission. Build the new one the way A1 built its role, create first and
permissions after, then wrap it in an instance profile, which is the object EC2 actually accepts.

```bash
aws iam create-role --role-name CWL-a2-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
aws iam attach-role-policy --role-name CWL-a2-role \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam put-role-policy --role-name CWL-a2-role --policy-name a2-s3 \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],"Resource":["arn:aws:s3:::BUCKET","arn:aws:s3:::BUCKET/*"]}]}'
aws iam create-instance-profile --instance-profile-name CWL-a2-role
aws iam add-role-to-instance-profile --instance-profile-name CWL-a2-role --role-name CWL-a2-role
```

`AmazonSSMManagedInstanceCore` is what lets you open a shell on the instance with no key pair and no
inbound rule. The inline policy is your bucket, and only your bucket. IAM takes about ten seconds to
propagate, so a `run-instances` fired immediately after the last line can come back saying the
instance profile does not exist. Wait, then run it again.

`CWL-a2-role` is the name to put in `INSTANCE_PROFILE` everywhere below.

## Get the data

Thirty-nine monthly yellow-taxi files, January 2022 to March 2025, and the zone lookup, straight
from the New York Taxi and Limousine Commission (TLC) distribution into your bucket. Run this in
CloudShell, before the Stage 1 instance exists. It moves about 2 GB and takes a few minutes, so start it before you read ahead. Each file
is deleted after it lands in S3, which keeps the local disk from filling.

```bash
for Y in 2022 2023 2024; do
  for M in 01 02 03 04 05 06 07 08 09 10 11 12; do
    curl -sSfO "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${Y}-${M}.parquet"
    aws s3 cp "yellow_tripdata_${Y}-${M}.parquet" s3://BUCKET/a2/raw/ --region ca-central-1
    rm "yellow_tripdata_${Y}-${M}.parquet"
  done
done

for M in 2025-01 2025-02 2025-03; do
  curl -sSfO "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${M}.parquet"
  aws s3 cp "yellow_tripdata_${M}.parquet" s3://BUCKET/a2/raw/ --region ca-central-1
  rm "yellow_tripdata_${M}.parquet"
done
```

The lookup ships as CSV. The Stage 2a job reads it as Parquet, with the columns `LocationID`,
`Borough`, and `Zone`, so convert it once before you need it.

```bash
curl -sSfO "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
uv run --with duckdb python3 -c "
import duckdb
duckdb.sql('''COPY (SELECT LocationID, Borough, Zone
                    FROM read_csv_auto('taxi_zone_lookup.csv'))
              TO 'zones.parquet' (FORMAT PARQUET)''')
"
aws s3 cp zones.parquet s3://BUCKET/a2/raw/zones.parquet --region ca-central-1
```

Check what landed before you launch anything. You want 40 objects: 39 trip files and
`zones.parquet`. A short count means a `curl` failed partway through the loop, so re-run it: the
files already in the bucket are simply overwritten.

```bash
aws s3 ls s3://BUCKET/a2/raw/ --summarize --human-readable --region ca-central-1
```

Prediction 1 starts from the size of the trip files, so total those 39 on their own. The lookup is
not part of what Stage 1 reads.

```bash
aws s3 ls s3://BUCKET/a2/raw/yellow_tripdata_ --summarize --human-readable --region ca-central-1
```

## Stage 1 on one EC2 instance

From CloudShell, launch one `m7g.xlarge` on Amazon Linux 2023 with the instance profile you made
under [Once per account](#once-per-account).

```bash
aws ec2 run-instances --region ca-central-1 \
  --image-id resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 \
  --instance-type m7g.xlarge \
  --count 1 \
  --subnet-id SUBNET \
  --iam-instance-profile Name=INSTANCE_PROFILE \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=60,VolumeType=gp3}' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=a2-stage1-CWL}]' \
  --query 'Instances[0].InstanceId' --output text
```

The root volume is sized at 60 GiB because the raw trip files pass through this box on their way
to S3, on top of the Python toolchain the run installs, and the AL2023 default of 8 GiB does not
leave room for both. `/dev/xvda` is AL2023's root device name. The image name ends in `arm64` because
an `m7g.xlarge` is a Graviton (ARM) machine, and an `x86_64` image does not boot on it. If you launch
from the console instead, pick the 64-bit (Arm) architecture for the same reason.

Connect from the EC2 console: select the instance, choose Connect, then the Session Manager tab.
There is no key pair and no inbound SSH rule anywhere in this assignment. The session opens in
`/usr/bin`, a folder you cannot write to, so the first line below moves to your home folder. Then
clone the repo and work from its root, because `stages/stage1_duckdb.py` imports a helper out of
`stages/stage2_spark.py` and a single copied file will not run.

```bash
cd ~
sudo dnf install -y git
curl -LsSf https://astral.sh/uv/install.sh | sh && source "$HOME/.local/bin/env"
git clone https://github.com/cpsc436c-2026w1/a2-starter.git && cd a2-starter
```

Now run the stage and keep the output.

```bash
uv run --python 3.11 \
  --with duckdb --with pyspark --with numpy --with "pandas<3" --with pyyaml \
  python3 stages/stage1_duckdb.py \
    --trips 's3://BUCKET/a2/raw/yellow_tripdata_*.parquet' \
    --out s3://BUCKET/a2/matrix.parquet \
    --window-start 2022-01-01 --window-end 2025-04-01 \
  | tee stage1.log
```

The `--trips` glob is quoted so DuckDB expands the `*`, not your shell. `--python 3.11` overrides
the system Python, which on Amazon Linux 2023 is 3.9. `pyspark`, `numpy`, and `pandas` are there
for one import at the top of the script and run none of the query.

If the run stops on a `Secret Validation Failure` before it prints anything, the instance has no
credentials to sign the S3 reads with: the instance profile is missing from the machine, so attach
`INSTANCE_PROFILE` to the instance and run it again.

Turn the log into a `results.csv` row. Confirm the metrics block arrived first: `grep -c
'===A2-METRICS' stage1.log` prints 1.

```bash
python3 stages/parse_run_log.py stage1.log --resource-id INSTANCE_ID --nodes 1
```

It prints the row to the shell and writes nothing. Copy what it prints into the `results.csv` on
your laptop, next to the prediction you put in that row before the run: the parser leaves
`predicted_s` empty every time, and your own copy of the file is the only place that number exists.

Leave the instance running if you want it for the S3 listings in Stage 2a's prediction blocks. It is the
cheapest thing in the assignment, and [Shut down and verify](#shut-down-and-verify) is where it dies.

## Launch the cluster

First check that the bucket holds the data and Stage 1's output, because the cluster logs to it and
every step reads from it. The bucket comes from [Once per account](#once-per-account), the data from
[Get the data](#get-the-data), and `matrix.parquet` from [Stage 1 on one EC2 instance](#stage-1-on-one-ec2-instance).

```bash
aws s3 ls s3://BUCKET/a2/        # expect raw/ and matrix.parquet
```

In the EMR console (**EMR on EC2 → Clusters → Create cluster**), set these and leave everything
else at its default:

- **Name and applications**: cluster name `a2-CWL`; Amazon EMR release `emr-spark-8.0.0`, picked
  exactly, since the console offers later `8.0.x` releases and those are different builds;
  application bundle **Spark**
- **Cluster configuration**: instance groups, with **Primary** 1 × `m7g.xlarge` and **Core**
  2 × `m7g.xlarge`; remove the Task group if the wizard offers one
- **Networking**: the subnet you saved as `SUBNET` under [Once per account](#once-per-account)
- **Cluster termination**: keep automatic termination after idle time on, and set it to
  **15 minutes**. It is the difference between a $2 assignment and a $60 one if you close your
  laptop at the wrong moment
- **Cluster logs**: `s3://BUCKET/a2/emr-logs/`
- **IAM roles**: service role `EMR_DefaultRole`, instance profile `EMR_EC2_DefaultRole`, the two
  that `create-default-roles` made

Choose **Create cluster**. On the cluster's page, choose **AWS CLI export** and paste the command
it shows into a new file in CloudShell, `launch-cluster.sh`.
With your values in place of the placeholders, it reads like this:

```bash
aws emr create-cluster --region ca-central-1 \
  --name "a2-CWL" \
  --release-label emr-spark-8.0.0 \
  --applications Name=Spark \
  --ec2-attributes SubnetId=SUBNET \
  --instance-groups \
      InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m7g.xlarge \
      InstanceGroupType=CORE,InstanceCount=2,InstanceType=m7g.xlarge \
  --use-default-roles \
  --auto-termination-policy IdleTimeout=900 \
  --log-uri s3://BUCKET/a2/emr-logs/
```

Check three things in the copy: the release label, `InstanceCount=2` on the CORE group, and
`IdleTimeout=900`. If the cluster terminates before you are done, run `bash launch-cluster.sh` in
CloudShell or repeat the console steps. Either builds the same cluster again.

The cluster id, `j-...`, is on the cluster page. Save it as `CLUSTER_ID`. Then poll until the
state reads `WAITING`. The same call returns the launch's start and end, so you do not time it
yourself.

```bash
aws emr describe-cluster --cluster-id CLUSTER_ID --region ca-central-1 \
  --query 'Cluster.Status.{State:State,Created:Timeline.CreationDateTime,Ready:Timeline.ReadyDateTime}'
```

`launch_s` is `Ready` minus `Created`, in seconds. The EMR console shows the same two stamps under
the cluster's timeline if you would rather read them there.

## Submit the job as a step

Upload the job script once. Every later run reads this same object, on both platforms.

```bash
aws s3 cp stages/stage2_spark.py s3://BUCKET/a2/stages/stage2_spark.py --region ca-central-1
```

Submit it as a step. `FIT_ROUNDS` comes from the book page. `--deploy-mode client` keeps the driver
on the primary node, which is what puts its stdout in the step's own log. The step passes no
partition argument, so the job runs at its own default of 16 shuffle partitions and prints that back
as `shuffle_partitions` in every metrics block. Both graded runs use the default, and so does
Stage 3.

```bash
aws emr add-steps --cluster-id CLUSTER_ID --region ca-central-1 \
  --steps 'Type=Spark,Name=a2-stage2-2n,ActionOnFailure=CONTINUE,Args=[--deploy-mode,client,s3://BUCKET/a2/stages/stage2_spark.py,--trips,s3://BUCKET/a2/matrix.parquet,--zones,s3://BUCKET/a2/raw/zones.parquet,--out,s3://BUCKET/a2/stage2-2n,--platform,emr,--fit-rounds,FIT_ROUNDS]' \
  --query 'StepIds[0]' --output text
```

Watch it, and note when it first reports `COMPLETED`.

```bash
aws emr describe-step --cluster-id CLUSTER_ID --step-id STEP_ID --region ca-central-1 \
  --query 'Step.Status.{State:State,Start:Timeline.StartDateTime,End:Timeline.EndDateTime}'
```

The script prints its metrics blocks on stdout, which EMR archives under your `--log-uri` a few
minutes after the step ends. Pull it down and unzip it.

```bash
aws s3 cp s3://BUCKET/a2/emr-logs/CLUSTER_ID/steps/STEP_ID/stdout.gz - --region ca-central-1 \
  | gunzip > stage2-2n.log
grep -c '===A2-METRICS' stage2-2n.log      # 2: one for A_aggregate, one for B_fit
```

If the object is not there yet, wait two minutes and repeat. Then parse both blocks into rows.

```bash
python3 stages/parse_run_log.py stage2-2n.log --resource-id CLUSTER_ID --nodes 2 \
  --startup-s LAUNCH_S
```

`LAUNCH_S` is the figure you just read off `describe-cluster`. It goes on the 2-node rows only,
because the 4-node run reuses the cluster the launch already bought. Both printed rows go into the
`results.csv` on your laptop, the same way Stage 1's did.

## Resize and re-run

Grow the cluster in place rather than launching a second one. Find the CORE group first.

```bash
aws emr describe-cluster --cluster-id CLUSTER_ID --region ca-central-1 \
  --query "Cluster.InstanceGroups[?InstanceGroupType=='CORE'].{Id:Id,Type:InstanceType,Running:RunningInstanceCount,State:Status.State}"
```

```bash
aws emr modify-instance-groups --region ca-central-1 \
  --instance-groups InstanceGroupId=IG_ID,InstanceCount=4
```

Re-run the same `describe-cluster` call until `Running` reads 4 and the group's state is
`RUNNING`. Submitting before then measures a 3-node cluster and quietly ruins the comparison.

Then wait about two more minutes before you submit. The instance group reports the machines as
running some way before Spark has an executor on each of them, and a step that starts early runs
part of phase A on half the cluster you are paying for.

The step's driver log settles it after the fact. The driver prints one `Registered executor` line
per executor, and all four have to arrive before the first stage is submitted. Fewer than four means
the wait was too short and the 4-node run is not comparable to the 2-node one. That log is
`stderr.gz`, beside the `stdout.gz` you pull the metrics blocks from.

```bash
aws s3 cp s3://BUCKET/a2/emr-logs/CLUSTER_ID/steps/STEP_ID/stderr.gz - --region ca-central-1 \
  | gunzip | grep -c 'Registered executor'      # 4
```

Then submit the identical step with a new output prefix.

```bash
aws emr add-steps --cluster-id CLUSTER_ID --region ca-central-1 \
  --steps 'Type=Spark,Name=a2-stage2-4n,ActionOnFailure=CONTINUE,Args=[--deploy-mode,client,s3://BUCKET/a2/stages/stage2_spark.py,--trips,s3://BUCKET/a2/matrix.parquet,--zones,s3://BUCKET/a2/raw/zones.parquet,--out,s3://BUCKET/a2/stage2-4n,--platform,emr,--fit-rounds,FIT_ROUNDS]' \
  --query 'StepIds[0]' --output text
```

```bash
aws s3 cp s3://BUCKET/a2/emr-logs/CLUSTER_ID/steps/STEP_ID/stdout.gz - --region ca-central-1 \
  | gunzip > stage2-4n.log
python3 stages/parse_run_log.py stage2-4n.log --resource-id CLUSTER_ID --nodes 4
```

Both runs carry the same `resource_id`, because both ran on the one cluster.

## Optional: re-run at 8 partitions

The ungraded extension in the assignment. Same cluster at 4 core nodes, same script, one extra
argument and a new output prefix so nothing already written is disturbed. The flag belongs to this
one run only: every graded run and the Glue run leave it off.

```bash
aws emr add-steps --cluster-id CLUSTER_ID --region ca-central-1 \
  --steps 'Type=Spark,Name=a2-stage2-4n-p8,ActionOnFailure=CONTINUE,Args=[--deploy-mode,client,s3://BUCKET/a2/stages/stage2_spark.py,--trips,s3://BUCKET/a2/matrix.parquet,--zones,s3://BUCKET/a2/raw/zones.parquet,--out,s3://BUCKET/a2/stage2-4n-p8,--platform,emr,--fit-rounds,FIT_ROUNDS,--shuffle-partitions,8]' \
  --query 'StepIds[0]' --output text
```

Pull the log the same way as the graded runs. This is the one run where `shuffle_partitions` and
`partitions` read 8 rather than 16, and the one run where a `retried=1`, or a lost executor and no
metrics block at all, is a result rather than a reason to rerun: fewer, larger partitions push a worker
harder. Keep these rows out of `results.csv`, or label them so a marker can see they are not one of
the 7 rows the submission asks for.

## Stage 3 on Glue

The script in S3 is the one EMR just ran, byte for byte. Only the submission changes.

`GLUE_ROLE` is a role Glue itself can assume. A fresh account has none (the 2026-08 pilot's
did not). Create it once, with the same create-then-attach move as the instance role:

```bash
aws iam create-role --role-name CWL-a2-glue-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
aws iam attach-role-policy --role-name CWL-a2-glue-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
aws iam put-role-policy --role-name CWL-a2-glue-role --policy-name a2-s3 \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],"Resource":["arn:aws:s3:::BUCKET","arn:aws:s3:::BUCKET/*"]}]}'
```

Create the job with 3 `G.1X` workers. The assignment says why that is the number.

```bash
aws glue create-job --region ca-central-1 \
  --name CWL-a2-stage3 \
  --role GLUE_ROLE \
  --glue-version 6.0 \
  --worker-type G.1X --number-of-workers 3 \
  --command Name=glueetl,ScriptLocation=s3://BUCKET/a2/stages/stage2_spark.py,PythonVersion=3 \
  --no-job-run-queuing-enabled \
  --default-arguments '{"--conf":"spark.ui.enabled=true","--enable-auto-scaling":"false"}'
```

Those are the only two default arguments the job takes. `--conf` is one key holding one string, so a
second Spark setting joins that same value (`"spark.ui.enabled=true --conf spark.other=x"`) rather
than arriving as a second key, which would overwrite the first.

`--enable-auto-scaling` stays `"false"`. With autoscaling on, the worker count moves during the run
and stops being the constant this whole comparison rests on. `spark.ui.enabled=true` keeps the
driver's REST endpoint alive, which is where the script reads its metrics from.

```bash
aws glue start-job-run --region ca-central-1 \
  --job-name CWL-a2-stage3 \
  --arguments '{"--trips":"s3://BUCKET/a2/matrix.parquet","--zones":"s3://BUCKET/a2/raw/zones.parquet","--out":"s3://BUCKET/a2/stage3","--platform":"glue","--fit-rounds":"FIT_ROUNDS"}' \
  --query 'JobRunId' --output text
```

The script's own flags travel as Glue job arguments, which is why they keep their `--` prefixes
here. Glue injects arguments of its own, `--JOB_NAME` among them, and the script ignores anything it
does not recognise.

Starting a second run the moment the first reports `SUCCEEDED` gets you
`ConcurrentRunsExceededException`. The state flips before the run's slot is released, and this job
is created with queuing off, so the new run is rejected rather than held. Wait about a minute and
start it again.

Read the timings off the run record.

```bash
aws glue get-job-run --job-name CWL-a2-stage3 --run-id JOB_RUN_ID --region ca-central-1 \
  --query 'JobRun.{State:JobRunState,Started:StartedOn,Completed:CompletedOn,Execution:ExecutionTime,DPUSeconds:DPUSeconds}'
```

`startup_s` is elapsed time minus `ExecutionTime`: take `Completed` minus `Started` in seconds and
subtract the execution seconds. The console prints the same figure as the run's start-up time. For
DPU-seconds, use the `DPUSeconds` field when it is populated, otherwise 3 workers times
`ExecutionTime`, since `G.1X` bills 1 DPU per worker.

The driver's stdout goes to the CloudWatch log group `/aws-glue/jobs/output`, in a stream named
for the run id exactly, with nothing on the end. That one stream is the driver's, and it holds both
metrics blocks. The executor streams live in `/aws-glue/jobs/error` under names like
`JOB_RUN_ID_g-<hash>`, and hold no metrics blocks.

```bash
aws logs get-log-events --region ca-central-1 \
  --log-group-name /aws-glue/jobs/output --log-stream-name JOB_RUN_ID \
  --start-from-head --output text --query 'events[*].message' \
  | tr '\t' '\n' > stage3.log
grep -c '===A2-METRICS' stage3.log         # 2
```

The `tr` puts one log message per line, which the parser needs. If the count comes back under 2, the
stream is paginated: open the same log group from the job-run page in the console and copy the
output from there. Then parse, with the startup seconds you just computed.

```bash
python3 stages/parse_run_log.py stage3.log --resource-id JOB_RUN_ID --nodes 3 --startup-s STARTUP_S
```

## Where the evidence lives

Three screenshots, and every id in them has to appear in `results.csv`.

| Screenshot | Page | Must be legible |
|---|---|---|
| 1 | EC2 → Instances → your instance | instance id, `m7g.xlarge`, launch time, CWL in the name |
| 2 | EMR → Clusters → your cluster | cluster id, release label, primary and core counts, CWL in the name |
| 3 | Glue → Jobs → your job → the run | run id, Glue version, `G.1X`, worker count, autoscaling off, start and end |

Take screenshot 2 while the cluster is alive and again after the resize, or take one after the
resize showing 4 core nodes and note the earlier count from your log. The step list on the same page
is worth capturing: it shows both runs on one cluster, which is the claim prediction 4 rests on.

Your account number should be visible in the console header: it is how we tell your work from
anyone else's. Upload screenshots to Canvas and nowhere else, and never screenshot a key, a
token, or a session credential.

## Shut down and verify

Check first that `results.csv` and your three screenshots are on your laptop. Terminating the
instance takes its disk with it, and nothing below can be undone.

In CloudShell:

```bash
aws emr terminate-clusters --cluster-ids CLUSTER_ID --region ca-central-1
aws ec2 terminate-instances --instance-ids INSTANCE_ID --region ca-central-1
```

Confirm both state changes on the console list pages. The Glue job costs nothing between runs, so
you can delete it or leave it.

```bash
aws glue delete-job --job-name CWL-a2-stage3 --region ca-central-1
```

Then check your budget the next morning, and again the morning after. A command that exits cleanly
proves the API accepted your request. The budget proves the meter stopped.
