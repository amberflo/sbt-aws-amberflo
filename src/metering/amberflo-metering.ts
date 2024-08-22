import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { aws_logs, Duration, Stack } from 'aws-cdk-lib';
import * as sbt from '@cdklabs/sbt-aws';
import * as path from 'path';

export interface AmberfloMeteringProps {
    /**
     * API Key from your Amberflo account.
     */
    readonly amberfloAPIKey: string

    /**
     * Amberflo base url
     */
    readonly amberfloBaseUrl: string
}

export class AmberfloMetering extends Construct implements sbt.IMetering {
    readonly createMeterFunction;
    readonly updateMeterFunction;
    readonly ingestUsageEventFunction;
    readonly cancelUsageEventsFunction;
    readonly fetchUsageFunction;

    constructor(scope: Construct, id: string, props: AmberfloMeteringProps) {
        super(scope, id);

        // https://docs.powertools.aws.dev/lambda/python/2.31.0/#lambda-layer
        const lambdaPowerToolsLayerARN = `arn:aws:lambda:${
          Stack.of(this).region
        }:017000801446:layer:AWSLambdaPowertoolsPythonV2:59`;

        /**
         * Creates the Amberflo Metering Lambda function.
         * The function is configured with the necessary environment variables,
         */
        const meteringService: lambda.IFunction = new lambda.Function(this, 'Service', {
            runtime: lambda.Runtime.PYTHON_3_12,
            handler: 'metering-service.handler',
            tracing: lambda.Tracing.ACTIVE,
            timeout: Duration.seconds(60),
            logGroup: new aws_logs.LogGroup(this, 'LogGroup', {
                retention: aws_logs.RetentionDays.FIVE_DAYS,
            }),
            code: lambda.Code.fromAsset(path.resolve(__dirname, '../../resources/functions')), // Path to the directory containing your Lambda function code
            layers: [
                lambda.LayerVersion.fromLayerVersionArn(this, 'LambdaPowerTools', lambdaPowerToolsLayerARN),
            ],
            environment: {
                AMBERFLO_API_KEY: props.amberfloAPIKey,
                AMBERFLO_BASE_URL: props.amberfloBaseUrl,
            },
        });

        this.createMeterFunction = meteringService;
        this.ingestUsageEventFunction = meteringService;
        this.fetchUsageFunction = meteringService;
        this.cancelUsageEventsFunction = meteringService;
        this.updateMeterFunction = meteringService;
    }
}
