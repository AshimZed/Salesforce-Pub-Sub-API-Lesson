import grpc
import requests
import threading
import io
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
import avro.schema
import avro.io
import time
import certifi
import json
import config

semaphore = threading.Semaphore(1) # Semaphore because Python's gRPC shuts down immediately after the call is made
                                   # and we need to keep the connection open to receive messages
                                   # This can be global because we only have one subcription

latest_replay_id = None

with open(certifi.where(), 'rb') as f:
    creds = grpc.ssl_channel_credentials(f.read())
with grpc.secure_channel('api.pubsub.salesforce.com:7443', creds) as channel:
    username = config.USERNAME
    password = config.PASSWORD
    url = 'https://awcomputing454.my.salesforce.com/services/Soap/u/59.0/'
    headers = {'content-type': 'text/xml', 'SOAPAction': 'login'}
    xml = f"""<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/'
    xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'
    xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body>
    <urn:login>
    <urn:username><![CDATA[{username}]]></urn:username>
    <urn:password><![CDATA[{password}]]></urn:password>
    </urn:login></soapenv:Body></soapenv:Envelope>"""
    res = requests.post(url, data=xml, headers=headers, verify=False)

    sessionid = config.SESSION_ID
    instanceurl = config.SERVER_URL
    tenantid = config.ORG_ID
    authmetadata = (('accesstoken', sessionid),
    ('instanceurl', instanceurl),
    ('tenantid', tenantid))
    stub = pb2_grpc.PubSubStub(channel)

    def fetchReqStream(topic):
        while True:
            semaphore.acquire()
            yield pb2.FetchRequest(
                topic_name = topic,
                replay_preset = pb2.ReplayPreset.LATEST,
                num_requested = 1)
            
    def decode(schema, payload):
        schema = avro.schema.parse(schema)
        buf = io.BytesIO(payload)
        decoder = avro.io.BinaryDecoder(buf)
        reader = avro.io.DatumReader(schema)
        ret = reader.read(decoder)
        return ret
    
    mysubtopic = "/data/AccountChangeEvent"
    print('Subscribing to ' + mysubtopic)
    substream = stub.Subscribe(fetchReqStream(mysubtopic),
            metadata=authmetadata)
    for event in substream:
        if event.events:
            semaphore.release()
            print("Number of events received: ", len(event.events))
            payloadbytes = event.events[0].event.payload
            schemaid = event.events[0].event.schema_id
            schema = stub.GetSchema(
                    pb2.SchemaRequest(schema_id=schemaid),
                    metadata=authmetadata).schema_json
            decoded = decode(schema, payloadbytes)
            entity_name = decoded['ChangeEventHeader']['entityName']
            change_type = decoded['ChangeEventHeader']['changeType']
            if change_type == 'CREATE':
                print("Welcome", decoded['Name'], "to the Johnny Spell's Family!")
        else:
            print("[", time.strftime('%b %d, %Y %I:%M%p %Z'),
                        "] The subscription is active.")
        latest_replay_id = event.latest_replay_id