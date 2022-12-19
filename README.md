# How to Build Production Ready Alternative Data Pipeline from DynamoDB to Apache Hudi with Streams and Firehose and Glue with Infrasture Code
* Create a batch-oriented pipeline to sync the data from your DynamoDB tables into Apache HUDI. This project assists you in bringing data from DynamoDB into Apache HUDI tables for analytical workloads and building data lake houses. DynamoDB can keep a year of operational data and history in Datalake. Customers that want to bring data where time is not an issue can run the Glue job at 1 hour or 1 day intervals based on their SLA and get data into Apache HUDI. This method is also less expensive than glue stream processing architecture that I demonstrated before. Again depending upon use cases and how fast you want the data and cost aspects choose your architecture.


![dynamodbstreams drawio](https://user-images.githubusercontent.com/39345855/208463603-7b93a4d5-1037-47fd-83c7-9d59e3a02c4a.png)


## Steps to Deploy Stack 

```
npx serverless plugin install -n serverless-lift
npx serverless plugin install -n serverless-dotenv-plugin
npx serverless plugin install -n serverless-plugin-resource-tagging
npx sls deploy
```

# I will add code soon 

# Streaming Architecture 
##### Build production Ready Real Time Transaction Hudi Datalake from DynamoDB Streams using Glue &kinesis
##### Video: https://www.youtube.com/watch?v=cWmRZ9WOZB8&t=464s
##### Code: https://github.com/soumilshah1995/dynamodb-hudi-stream-project


Also, I have a full HUDI playlist on YouTube with over 15+ videos about Apache HUDI. If you have any questions about HUDI or Glue, please reach out. I am delighted to connect and discuss more.

#### Playlist on HUDI 
* https://www.youtube.com/watch?v=5zF4jc_3rFs&list=PLL2hlSFBmWwwbMpcyMjYuRn8cN99gFSY6



##### JOIN Apache Hudi Slack and meet new people  https://join.slack.com/t/apache-hudi/shared_invite/zt-1e94d3xro-JvlNO1kSeIHJBTVfLPlI5w

* This are all real world production ready project that you will be working as senior data engineer. Feel free to try out these projects. A step by step video will be out soon 




