# UWaterloo Facility Notifier

Lambda function to send Discord/Telegram notifications when the UWaterloo facility calendar changes

Discord example:
<img width="453" alt="image" src="https://user-images.githubusercontent.com/5977478/214961417-269239d0-7453-403e-962f-5e0a13f14d1a.png">

Telegram example:
![image](https://user-images.githubusercontent.com/5977478/221892890-ebe99657-fbb3-4bf7-b6c4-848e053e480e.jpeg)



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
3. (Optional, for Discord notifications) Create a Discord webhook and save it as `DISCORD_WEBHOOK_URLS` environment variable in AWS Lambda.
4. (Optional, for Telegram notifications) Create a Telegram bot and save its token as `TELEGRAM_BOT_TOKEN` environment variable in AWS Lambda. Add the bot to a group chat and interact with it to subscribe to notifications. The bot replies every time the lambda function is invoked.

Upload the `.zip` file to the lambda function.
