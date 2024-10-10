import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { aws_logs as AwsLogs, Duration, Stack } from 'aws-cdk-lib';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as sbt from '@cdklabs/sbt-aws';
import * as path from 'path';

export interface AmberfloMeteringProps {
    /**
     * The name of the AWS Secrets Manager secret that contains the Amberflo API Key.
     * This is the identifier for the secret in Secrets Manager.
     */
    readonly amberfloAPIKeySecretName: string

    /**
     * The key within the AWS Secrets Manager secret that identifies the Amberflo API Key.
     * This is the field name within the JSON structure of the secret.
     */
    readonly amberfloAPIKeySecretId: string

    /**
     * Amberflo base url
     */
    readonly amberfloBaseUrl?: string
}

export class AmberfloMetering extends Construct implements sbt.IMetering {
    readonly createMeterFunction;
    readonly fetchMeterFunction;
    readonly fetchAllMetersFunction;
    readonly updateMeterFunction;
    readonly deleteMeterFunction;
    readonly ingestUsageEventFunction;
    readonly fetchUsageFunction;
    readonly cancelUsageEventsFunction;

    constructor(scope: Construct, id: string, props: AmberfloMeteringProps) {
        super(scope, id);
        sbt.addTemplateTag(this, 'AmberfloMetering');

        const amberfloBaseUrl = props.amberfloBaseUrl || 'https://app.amberflo.io';

        // https://docs.powertools.aws.dev/lambda/python/2.31.0/#lambda-layer
        const lambdaPowerToolsLayerARN = `arn:aws:lambda:${
            Stack.of(this).region
        }:017000801446:layer:AWSLambdaPowertoolsPythonV2:59`;

        // https://repost.aws/questions/QUlA3-nvrmTbGpnnhE-vRf0g/python-layers-and-requests-import-module
        const requestsModuleLayerARN = `arn:aws:lambda:${
            Stack.of(this).region
        }:770693421928:layer:Klayers-p312-requests:8`;

        /**
         * Creates the Amberflo Metering Lambda function.
         * The function is configured with the necessary environment variables,
         */
        const meteringService: lambda.IFunction = new lambda.Function(this, 'Service', {
            runtime: lambda.Runtime.PYTHON_3_12,
            handler: 'metering-service.handler',
            tracing: lambda.Tracing.ACTIVE,
            timeout: Duration.seconds(60),
            logGroup: new AwsLogs.LogGroup(this, 'LogGroup', {
                retention: AwsLogs.RetentionDays.FIVE_DAYS,
            }),
            code: lambda.Code.fromAsset(path.resolve(__dirname, '../../resources/functions')), // Path to the directory containing your Lambda function code
            layers: [
                lambda.LayerVersion.fromLayerVersionArn(this, 'LambdaPowerTools', lambdaPowerToolsLayerARN),
                lambda.LayerVersion.fromLayerVersionArn(this, 'requestsModule', requestsModuleLayerARN),
            ],
            environment: {
                API_KEY_SECRET_NAME: props.amberfloAPIKeySecretName,
                API_KEY_SECRET_ID: props.amberfloAPIKeySecretId,
                AMBERFLO_BASE_URL: amberfloBaseUrl,
            },
        });

        const meteringSyncFunction: sbt.ISyncFunction = {
            handler: meteringService
        };
        const meteringAsyncFunction: sbt.IASyncFunction = {
            handler: meteringService
        };

        // grant permission to read amberfloAPIKey secret
        const amberfloApiKeySecret = secretsmanager.Secret.fromSecretNameV2(this, 'AmberfloApiKeySecret', props.amberfloAPIKeySecretName);
        amberfloApiKeySecret.grantRead(meteringService);

        this.createMeterFunction = meteringSyncFunction;
        this.fetchMeterFunction = meteringSyncFunction;
        this.fetchAllMetersFunction = meteringSyncFunction;
        this.updateMeterFunction = meteringSyncFunction;
        this.deleteMeterFunction = meteringSyncFunction;
        this.ingestUsageEventFunction = meteringAsyncFunction;
        this.fetchUsageFunction = meteringAsyncFunction;
        this.cancelUsageEventsFunction = meteringSyncFunction;
    }
}
