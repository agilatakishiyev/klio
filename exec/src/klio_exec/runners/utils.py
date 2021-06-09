# Copyright 2021 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import concurrent.futures
import logging
import time
import threading

import apache_beam as beam

from apache_beam import pipeline
from apache_beam.io.gcp import pubsub as beam_pubsub
from apache_beam.options import pipeline_options
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.direct import transform_evaluator
from apache_beam.utils import timestamp as beam_timestamp
from google.cloud import pubsub as g_pubsub

from klio_core.proto import klio_pb2


_LOGGER = logging.getLogger("klio")

ENTITY_ID_TO_ACK_ID = {}



class PubSubKlioMessage:

    def __init__(self, ack_id, kmsg_id):
        self.ack_id = ack_id
        self.kmsg_id = kmsg_id
        self.last_extended = None
        self.last_extended_for = None
        self.event = threading.Event()

    def extend(self, duration):
        self.last_extended = time.monotonic()
        self.last_extended_for = duration

    def __repr__(self):
        return f"PubSubKlioMessage(kmsg_id={self.kmsg_id})"


class MessageManager:
    DEFAULT_DEADLINE_EXTENSION = 10
    # DEFAULT_DEADLINE_EXTENSION = 60 * 5  # 5 minutes

    # TODO: increase manager & heartbeat sleep when ready

    def __init__(self, sub_name, heartbeat_sleep=1, manager_sleep=0.1):
        # can't re-use client since the other class closes the channel
        self._client = g_pubsub.SubscriberClient()
        self._sub_name = sub_name
        self.heartbeat_sleep = heartbeat_sleep
        self.manager_sleep = manager_sleep
        self.messages = []
        self.messages_lock = threading.Lock()
        self.logger = logging.getLogger("klio.message_manager")
        self.start_threads()

    def start_threads(self):
        mgr_thread = threading.Thread(
            target=self.manage, 
            args=(self.manager_sleep,), 
            name="KlioMessageManager",
            daemon=True,
        )
        mgr_thread.start()

        heartbeat_thread = threading.Thread(
            target=self.heartbeat, 
            args=(self.heartbeat_sleep,), 
            name="KlioMessageHeartbeat",
            daemon=True,
        )
        heartbeat_thread.start()

    def manage(self, to_sleep):
        while True:
            to_remove = []
            with self.messages_lock:
                for message in self.messages:
                    diff = 0
                    if not message.event.is_set():
                        now = time.monotonic()
                        if message.last_extended is not None:
                            diff = now - message.last_extended
                        if message.last_extended is None or diff >= (self.DEFAULT_DEADLINE_EXTENSION - 8):
                            self.extend_deadline(message)
                        else:
                            self.logger.warning(f"Skipping extending deadline {message}")
                    else:
                        self.logger.warning(f"XX message manager going to remove {message}")
                        to_remove.append(message)

            for message in to_remove:
                self.logger.warning(f"Is event set for {message.kmsg_id}? {message.event.is_set()}")
                self.remove(message)
            
            time.sleep(to_sleep)
            
    def heartbeat(self, to_sleep):
        while True:
            for message in self.messages:
                self.logger.warning(f"Still processing {message.kmsg_id}...")
            time.sleep(to_sleep)
        
    def extend_deadline(self, message, duration=None):
        if duration is None:
            duration = self.DEFAULT_DEADLINE_EXTENSION
        request = {
            "subscription": self._sub_name,
            "ack_ids": [message.ack_id],
            "ack_deadline_seconds": duration,  # seconds
        }
        # TODO: this method also has `retry` and `timeout` kwargs which
        # we may be interested in using
        self._client.modify_ack_deadline(**request)
        message.extend(duration)
        self.logger.warning(f"Extended deadline for {message} by {duration}")

    def add(self, message):
        self.logger.warning(f"Received {message.kmsg_id}")
        self.logger.warning(f"Message event status for {message.kmsg_id}: {message.event.is_set()}")
        self.extend_deadline(message)
        ENTITY_ID_TO_ACK_ID[message.kmsg_id] = message
        with self.messages_lock:
            self.messages.append(message)

    def remove(self, message):
        # TODO: this method also has `retry`, `timeout` and metadata 
        # kwargs which we may be interested in using
        self._client.acknowledge(self._sub_name, [message.ack_id])
        self.logger.warning(f"Acknowledged {message.kmsg_id}.")
        index = self.messages.index(message)
        with self.messages_lock:
            self.messages.pop(index)
        ENTITY_ID_TO_ACK_ID.pop(message.kmsg_id)
        self.logger.warning(f"Done processing {message.kmsg_id}.")


class KlioPubSubReadEvaluator(transform_evaluator._PubSubReadEvaluator):

    def __init__(self, *args, **kwargs):
        super(KlioPubSubReadEvaluator, self).__init__(*args, **kwargs)
        # Heads up: self._sub_name is from init'ing parent class
        self.sub_client = g_pubsub.SubscriberClient()
        self.message_manager = MessageManager(self._sub_name)

    def _read_from_pubsub(self, timestamp_attribute):

        def _get_element(ack_id, message):
            parsed_message = beam_pubsub.PubsubMessage._from_message(message)
            if (timestamp_attribute and timestamp_attribute in parsed_message.attributes):
                rfc3339_or_milli = parsed_message.attributes[timestamp_attribute]
                try:
                    timestamp = beam_timestamp.Timestamp(micros=int(rfc3339_or_milli) * 1000)
                except ValueError:
                    try:
                        timestamp = beam_timestamp.Timestamp.from_rfc3339(rfc3339_or_milli)
                    except ValueError as e:
                        raise ValueError('Bad timestamp value: %s' % e)
            else:
                timestamp = beam_timestamp.Timestamp(
                    message.publish_time.seconds, message.publish_time.nanos // 1000)

            # TODO: either use klio.mssage.serializer.to_klio_message, or 
            # figure out how to handle when a parsed_message can't be parsed
            # into a KlioMessage
            kmsg = klio_pb2.KlioMessage()
            kmsg.ParseFromString(parsed_message.data)
            entity_id = kmsg.data.element.decode("utf-8")

            msg = PubSubKlioMessage(ack_id, entity_id)
            self.message_manager.add(msg)

            return timestamp, parsed_message

        try:
            response = self.sub_client.pull(self._sub_name, max_messages=1, return_immediately=True)
            results = [_get_element(rm.ack_id, rm.message) for rm in response.received_messages]

        finally:
            # TODO: does this need to be closed?
            self.sub_client.api.transport.channel.close()

        return results


class KlioTransformEvaluatorRegistry(transform_evaluator.TransformEvaluatorRegistry):
    
    def __init__(self, *args, **kwargs):
        super(KlioTransformEvaluatorRegistry, self).__init__(*args, **kwargs)
        self._evaluators[direct_runner._DirectReadFromPubSub] = KlioPubSubReadEvaluator


class KlioAck(beam.DoFn):
    def __init__(self):
        self.logger = logging.getLogger("klio.message_manager")

    def process(self, element):
        self.logger.warning("IN KLIO ACK OVERRIDE!")

        kmsg = klio_pb2.KlioMessage()
        kmsg.ParseFromString(element)
        entity_id = kmsg.data.element.decode("utf-8")
        # with LOCK:
        msg = ENTITY_ID_TO_ACK_ID.get(entity_id)
        self.logger.warning(f"XXX GOT ack_id {msg} for {entity_id}!")
        msg.event.set()

        yield element


# this class isn't needed; it's just to know that the composite override is working
class _KlioDirectWriteToPubSubFn(direct_runner._DirectWriteToPubSubFn):
    def _flush(self):
        _LOGGER.warning(f"FLUSHINGGG")
        return super(_KlioDirectWriteToPubSubFn, self)._flush()


class _KlioAckAndWrite(beam.PTransform):
    def __init__(self, override):
        self.override = override

    def expand(self, pcoll):
        return (
            pcoll
            | beam.ParDo(KlioAck())
            | beam.ParDo(_KlioDirectWriteToPubSubFn(self.override))
        )


class KlioWriteToPubSubOverride(pipeline.PTransformOverride):
    def matches(self, applied_ptransform):
        return isinstance(applied_ptransform.transform, beam_pubsub.WriteToPubSub)

    def get_replacement_transform_for_applied_ptransform(self, applied_ptransform):
        return _KlioAckAndWrite(applied_ptransform.transform)

