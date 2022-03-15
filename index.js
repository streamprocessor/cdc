
const {PubSub} = require('@google-cloud/pubsub');
require('@google-cloud/functions-framework');

const pubSubClient = new PubSub();
const topicName = 'test';

function getChangeType(change) {
    if (!change.value.hasOwnProperty('fields')) {
        return "DELETE";
    }
    if (!change.oldValue.hasOwnProperty('fields')) {
        return "CREATE";
    }
    return "UPDATE";
}

function getChangeDocument(change) {
  if (change.value.hasOwnProperty('fields')) {
      return change.value;
  }
  else {
      return change.oldValue;
  }
}

exports.cdcFirestore = async (change, context) => {
  const resource = context.resource;
  console.log('Function triggered by change to: ' +  resource);
  try {
    const document = getChangeDocument(change);
    console.log(JSON.stringify(document, null, 4));
    const data =
      {
          timestamp: context.timestamp,
          operation: getChangeType(change),
          eventId: context.eventId,
          identity: document.fields.identity.stringValue,
          token: document.fields.token.stringValue ?? parseFloat(document.fields.token.doubleValue) ?? parseInt(document.fields.token.integerValue),
          value: document.fields.value.stringValue ?? document.fields.value.doubleValue ?? document.fields.value.integerValue,
        };
        console.log(JSON.stringify(data, null, 4));
        
        const dataBuffer = Buffer.from(JSON.stringify(data));

    try {
        const messageId = await pubSubClient.topic(topicName).publishMessage({data:dataBuffer});
        console.log(`Message ${messageId} published.`);
    } catch (error) {
        console.error(`Received error while publishing: ${error.message}`);
        process.exitCode = 1;
    }  
  }
  catch (err) {
      console.error(err);
  }
};