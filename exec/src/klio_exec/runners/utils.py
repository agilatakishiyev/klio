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

LOCK = threading.Lock()
ENTITY_ID_TO_ACK_ID = {}



class PubSubKlioMessage:
    def __init__(self, ack_id, kmsg_id):
        self.ack_id = ack_id
        self.kmsg_id = kmsg_id
        self.last_extended = None
        self.last_extended_for = None
        self.event = threading.Event()

    def __repr__(self):
        return f"PubSubKlioMessage(kmsg_id={self.kmsg_id})"

    def extend(self, duration):
        self.last_extended = time.monotonic()
        self.last_extended_for = duration


class MessageManager:
    DEFAULT_DEADLINE_EXTENSION = 10
    # DEFAULT_DEADLINE_EXTENSION = 60 * 5  # 5 minutes

    # TODO: increase manager sleep

    def __init__(self, sub_name, heartbeat_sleep=10, manager_sleep=0.1):
        # can't re-use client since the other class closes the channel
        self._client = g_pubsub.SubscriberClient()
        self._sub_name = sub_name
        self.heartbeat_sleep = heartbeat_sleep
        self.manager_sleep = manager_sleep
        self.messages = []
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

    def manage(self, message):
        diff = 0
        while not message.event.is_set():
            now = time.monotonic()
            if message.last_extended is not None:
                diff = now - message.last_extended
            if message.last_extended is None or diff >= (self.DEFAULT_DEADLINE_EXTENSION - 60):
                self.extend_deadline(message)
            else:
                _LOGGER.warning(f"Skipping extending deadline {message}")

            time.sleep(self.manager_sleep)
        else:
            _LOGGER.warning(f"No longer extending deadline for {message}")
            self.remove(message)
        
    def extend_deadline(self, message, duration=None):
        if duration is None:
            duration = self.DEFAULT_DEADLINE_EXTENSION
        request = {
            "subscription": self._sub_name,
            "ack_ids": [message.ack_id],
            "ack_deadline_seconds": duration,  # seconds
        }
        # TODO: this method also has `retry` and `timeout` kwargs which
        # we may be interested
        self._client.modify_ack_deadline(**request)
        message.extend(duration)
        _LOGGER.warning(f"Extended deadline for {message} by {duration}")

    def add(self, message):
        _LOGGER.warning(f"Received {message.kmsg_id}")
        _LOGGER.warning(f"Message event status for {message.kmsg_id}: {message.event.is_set()}")
        self.extend_deadline(message)
        ENTITY_ID_TO_ACK_ID[message.kmsg_id] = message
        self.executor.submit(self.manage, message)

    def remove(self, message):
        # TODO: this method also has `retry`, `timeout` and metadata 
        # kwargs which we may be interested
        self._client.acknowledge(self._sub_name, [message.ack_id])
        _LOGGER.warning(f"Acknowledged {message.kmsg_id}.")
        with LOCK:
            ENTITY_ID_TO_ACK_ID.pop(message.kmsg_id)
        _LOGGER.warning(f"Done processing {message.kmsg_id}.")


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


class _KlioDirectWriteToPubSubFn(beam.DoFn):
    BUFFER_SIZE_ELEMENTS = 1
    FLUSH_TIMEOUT_SECS = BUFFER_SIZE_ELEMENTS * 0.01

    def __init__(self, transform):
        self.project = transform.project
        self.short_topic_name = transform.topic_name
        self.id_label = transform.id_label
        self.timestamp_attribute = transform.timestamp_attribute
        self.with_attributes = transform.with_attributes

        # TODO(BEAM-4275): Add support for id_label and timestamp_attribute.
        if transform.id_label:
            raise NotImplementedError(
                'DirectRunner: id_label is not supported for '
                'PubSub writes')
        if transform.timestamp_attribute:
            raise NotImplementedError(
                'DirectRunner: timestamp_attribute is not '
                'supported for PubSub writes')

    def setup(self):
        self.pub_client = g_pubsub.PublisherClient()
        self.topic = self.pub_client.topic_path(self.project, self.short_topic_name)

    def start_bundle(self):
        self._buffer = []

    def process(self, elem):
        self._buffer.append(elem)
 
        # TODO: either use handle_klio, or 
        # figure out how to handle when a parsed_message can't be parsed
        # into a KlioMessage
        kmsg = klio_pb2.KlioMessage()
        kmsg.ParseFromString(elem)
        entity_id = kmsg.data.element.decode("utf-8")
        with LOCK:
            msg = ENTITY_ID_TO_ACK_ID.get(entity_id)
            _LOGGER.warning(f"XXX GOT ack_id {id(msg)} for {entity_id}!")
            _LOGGER.warning(f"XXX dir(msg) {dir(msg)} for {entity_id}!")
            msg.event.set()

        future = self.pub_client.publish(self.topic, elem)
        future.result()
        # if len(self._buffer) >= self.BUFFER_SIZE_ELEMENTS:
        #     self._flush()

    def finish_bundle(self):
        self._flush()

    def _flush(self):
        _LOGGER.warning("**** KLIO FLUSHINGGGG")
        # from google.cloud import pubsub
        # pub_client = pubsub.PublisherClient()
        # topic = pub_client.topic_path(self.project, self.short_topic_name)

        if self.with_attributes:
            futures = [
                self.pub_client.publish(self.topic, elem.data, **elem.attributes)
                for elem in self._buffer
            ]
        else:
            futures = [self.pub_client.publish(self.topic, elem) for elem in self._buffer]

        timer_start = time.time()
        for future in futures:
            remaining = self.FLUSH_TIMEOUT_SECS - (time.time() - timer_start)
            future.result(remaining)
        self._buffer = []


class KlioWriteToPubSubOverride(pipeline.PTransformOverride):
    def matches(self, applied_ptransform):
        return isinstance(applied_ptransform.transform, beam_pubsub.WriteToPubSub)

    def get_replacement_transform_for_applied_ptransform(self, applied_ptransform):
        return beam.ParDo(_KlioDirectWriteToPubSubFn(applied_ptransform.transform))