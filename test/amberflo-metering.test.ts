import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { AmberfloMetering } from "../src/metering/amberflo-metering";

test('Moesif Billing Management Lambdas Created', () => {
    const app = new cdk.App();
    const stack = new cdk.Stack(app, "amberflo-test-stack");
    new AmberfloMetering(stack, 'AmberfloMetering', {
        amberfloAPIKeySecretName: 'AmberfloApiKey',
        amberfloAPIKeySecretId: 'AmberfloApiKey',
        amberfloBaseUrl: '<<Amberflo Base URL>>'
    });
    Template.fromStack(stack);
});
