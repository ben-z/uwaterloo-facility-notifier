# UWaterloo Facility Notifier

### Testing

Install dependencies:

```bash
pip install -r requirements.txt
```

Copy `.env.example` to `.env`. Populate credentials.

Invoke `lambda_function` in the VSCode debugger.


### Deployment

Create a `.zip` file for deployment

```bash
./create_deployment_package.sh
```

Create an AWS Lambda function with the following requirements:
1. In addition to the default `AWSLambdaBasicExecutionRole`, add `AmazonDynamoDBFullAccess` for access to DynamoDB.
2. Add a `CloudWatch Events` trigger and set the schedule expression appropriately (e.g. `rate(5 minutes)`)
3. Create a Discord webhook and save it as `DISCORD_WEBHOOK_URLS` environment variable in AWS Lambda.

Upload the `.zip` file to the lambda function.
