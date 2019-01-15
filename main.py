# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import json
import random
import time
import sys
import iothub_client
import base64
import codecs
# pylint: disable=E0611
from iothub_client import IoTHubModuleClient, IoTHubClientError, IoTHubTransportProvider
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubModuleClient.send_event_async.
# By default, messages do not expire.
MESSAGE_TIMEOUT = 10000

# global counters
RECEIVE_CALLBACKS = 0
SEND_CALLBACKS = 0
TWIN_CALLBACKS = 0

# Twin Payload store
TwinPayload = {}
# latest messages JSON
last_messages = {}

# Choose HTTP, AMQP or MQTT as transport protocol.  Currently only MQTT is supported.
PROTOCOL = IoTHubTransportProvider.MQTT

# module_twin_callback is invoked when the module twin's desired properties are updated.
def module_twin_callback(update_state, payload, user_context):
    global TWIN_CALLBACKS
    global TwinPayload
    print ( "\nTwin callback called with:\nupdateStatus = %s\npayload = %s\ncontext = %s" % (update_state, payload, user_context) )
    TwinPayload = json.loads(payload)
    print( "TwinPayload: %s\n" % TwinPayload)
    TWIN_CALLBACKS += 1
    print ( "Total calls confirmed: %d\n" % TWIN_CALLBACKS )

# Callback received when the message that we're forwarding is processed.
def send_confirmation_callback(message, result, user_context):
    global SEND_CALLBACKS
    print ( "Confirmation[%d] received for message with result = %s" % (user_context, result) )
    map_properties = message.properties()
    key_value_pair = map_properties.get_internals()
    print ( "    Properties: %s" % key_value_pair )
    SEND_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % SEND_CALLBACKS )

def check_prev(message_json):
    global last_messages
    output = None
    iden = message_json['id']
    # check if id is present in last_messages json
    if iden in last_messages:
        # check if last estado is different from incoming and prepare for output
        if last_messages[iden]['estado'] != message_json['estado']:
            output = message_json
            print("diferente mensaje")
        else:
            print("es igual")
    else:
        output = message_json
        print("nuevo mensaje")
    # fill last message json for id
    last_messages[iden] = {}
    last_messages[iden]['time'] = message_json['time']
    last_messages[iden]['estado'] = message_json['estado']
    
    return output

def process_elevacion_type(message_lora, hubManager):
    global TwinPayload
    json_obj = {}
    deveui = message_lora["deveui"]
    # decode data
    data_decoded = base64.b64decode(message_lora["data"])
    data_decoded = data_decoded.decode('unicode_escape')
    # save decoded data as json object
    data_decoded_json = json.loads(data_decoded)

    json_obj['time'] = message_lora['time']
    
    # send message for first id
    if ('id' in TwinPayload["desired"]["devices"][deveui] and ("stateA" in data_decoded_json)):
        json_obj["estado"] = data_decoded_json["stateA"]
        json_obj['id'] = TwinPayload["desired"]["devices"][deveui]["id"]
        json_obj_ = check_prev(json_obj)
        # check previous
        if json_obj_ != None:
            new_message = json.dumps(json_obj_)
            new_message = IoTHubMessage(new_message)
            hubManager.forward_event_to_output("output1", new_message, 0)

    # send separate message for same-deveui/different-id scenario
    if (("id2" in TwinPayload["desired"]["devices"][deveui]) and ("stateB" in data_decoded_json)):
        json_obj["estado"] = data_decoded_json["stateB"]
        json_obj["id"] = TwinPayload["desired"]["devices"][deveui]["id2"]
        json_obj_ = check_prev(json_obj)
        # check previous
        if json_obj_ != None:
            new_message = json.dumps(json_obj_)
            new_message = IoTHubMessage(new_message)
            hubManager.forward_event_to_output("output1", new_message, 0)

# receive_message_callback is invoked when an incoming message arrives on the specified 
# input queue (in the case of this sample, "input1").  Because this is a filter module, 
# we will forward this message onto the "output1" queue.
def receive_message_callback(message, hubManager):
    global RECEIVE_CALLBACKS
    global TwinPayload
    # Decode Conduit Downstream Device message
    message_buffer = message.get_bytearray()
    size = len(message_buffer)
    message_text = message_buffer[:size].decode('utf-8')
    print ( "    Data: <<<%s>>> & Size=%d" % (message_text, size) )
    #map_properties = message.properties()
    #key_value_pair = map_properties.get_internals()
    #print ( "    Properties: %s" % key_value_pair )
    RECEIVE_CALLBACKS += 1
    print ( "    Total calls received: %d" % RECEIVE_CALLBACKS )
    
    # lora messages loaded to json object.
    message_lora = json.loads(message_text)
    # save incoming deveui
    deveui = message_lora["deveui"]
    
    # look for device declaration if any
    if deveui in TwinPayload["desired"]["devices"]:
        #if 'tipo' in TwinPayload["desired"]["devices"][deveui]:
        process_elevacion_type(message_lora, hubManager)
    else:
        # send event to Hub
        hubManager.forward_event_to_output("output1", message, 0)
        
    return IoTHubMessageDispositionResult.ACCEPTED


class HubManager(object):

    def __init__(
            self,
            protocol=IoTHubTransportProvider.MQTT):
        self.client_protocol = protocol
        self.client = IoTHubModuleClient()
        self.client.create_from_environment(protocol)

        # set the time until a message times out
        self.client.set_option("messageTimeout", MESSAGE_TIMEOUT)
        
        # sets the callback when a message arrives on "input1" queue.  Messages sent to 
        # other inputs or to the default will be silently discarded.
        self.client.set_message_callback("input1", receive_message_callback, self)

        # Sets the callback when a module twin's desired properties are updated.
        self.client.set_module_twin_callback(module_twin_callback, self)

    # Forwards the message received onto the next stage in the process.
    def forward_event_to_output(self, outputQueueName, event, send_context):
        self.client.send_event_async(
            outputQueueName, event, send_confirmation_callback, send_context)

def main(protocol):
    try:
        print ( "\nPython %s\n" % sys.version )
        print ( "IoT Hub Client for Python" )

        hub_manager = HubManager(protocol)

        print ( "Starting the IoT Hub Python sample using protocol %s..." % hub_manager.client_protocol )
        print ( "The app is now waiting for messages and will indefinitely.  Press Ctrl-C to exit. ")

        while True:
            time.sleep(1)

    except IoTHubError as iothub_error:
        print ( "Unexpected error %s from IoTHub" % iothub_error )
        return
    except KeyboardInterrupt:
        print ( "IoTHubModuleClient sample stopped" )

if __name__ == '__main__':
    main(PROTOCOL)