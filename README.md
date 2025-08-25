# Dr. Tavel Screening Environment
This repo is setup based upon the example avialable here: https://github.com/aws-samples/aws-sam-github-actions-example/tree/main

# The Test
This test is designed to be open ended and does not have any specific tasks. The main goal is to test your instincts of how to improve existing systems. You can bring up anything you don't like or would change about this repo. This includes (but is not limited to):
- We should improve the CI/CD by adding "X" to the workflow
- We should improve the local testing setup by doing "Y"
- We can speed up the pipeline by removing "Z"
- We should add a security scanning service
- We should add more observability or alerting, for example: ...
- The tests do not have value because: ...

Be as specific about changes as possible. Please do not write any code or open any issues. You can send a markdown file with your reccomendations to Fred. There should be enough detail that someone else could pick up the ticket and do it.

Assume that what you see in the repo deploys and runs. Also assume that what you see in the repo is the entire stack of the microservice. You will not be marked down for missing any runtime issues. Focus on improvements to the architecture, infrastructure, pipeline, developer experience, and testing. 

This test is exactly what you will be doing on the job! You will need to improve upon existing systems in a safe and efficient way.

# AI Tools
You can use AI tools or any tools you would use on the job.

# Example: Observability and Monitoring Improvements
## Problem
The observability of the lambda function is missing some important pieces. When the lambda function fails, there are no alarms that alert the developers. This change is critically important because the lambda function could be silently failing right now and we wouldn't know unless we looked at the lambda's metrics.

## Solution
The solution to this is to implement CloudWatch Alarms. These can be implemented by adding them to the `template.yaml` file. THe documentation for this is [here](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-cloudwatch-alarm.html). We should alarm on:
- any errors
- when the duration of the lambda spikes in an anomyolous way
- when the lambda function is throttled