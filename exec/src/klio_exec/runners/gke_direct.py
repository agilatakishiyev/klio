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

import logging
import warnings

warnings.simplefilter("ignore")

from apache_beam.options.pipeline_options import DirectOptions
from apache_beam.runners.direct.bundle_factory import BundleFactory
from apache_beam.runners.direct.clock import RealClock
from apache_beam.runners.direct.clock import TestClock
from apache_beam.runners.direct import direct_runner

from apache_beam.pipeline import PipelineVisitor
from apache_beam.runners.direct.consumer_tracking_pipeline_visitor import ConsumerTrackingPipelineVisitor
from apache_beam.runners.direct.evaluation_context import EvaluationContext
from apache_beam.runners.direct.executor import Executor
from apache_beam.testing.test_stream import TestStream
from apache_beam.options.value_provider import RuntimeValueProvider

from klio_exec.runners import utils


_LOGGER = logging.getLogger("klio")


def _update_overrides(overrides):
    updated_overrides = []
    for override in overrides:
        name = override.__class__.__name__
        if name != "WriteToPubSubOverride":
            updated_overrides.append(override)
        else:
            write_pubsub_override = utils.KlioWriteToPubSubOverride()
            updated_overrides.append(write_pubsub_override)

    return updated_overrides


class GkeDirectRunner(direct_runner.BundleBasedDirectRunner):

    def run_pipeline(self, pipeline, options):
        """Execute the entire pipeline and returns an DirectPipelineResult."""

        # If the TestStream I/O is used, use a mock test clock.
        class TestStreamUsageVisitor(PipelineVisitor):
            """Visitor determining whether a Pipeline uses a TestStream."""
            def __init__(self):
                self.uses_test_stream = False

            def visit_transform(self, applied_ptransform):
                if isinstance(applied_ptransform.transform, TestStream):
                    self.uses_test_stream = True

        visitor = TestStreamUsageVisitor()
        pipeline.visit(visitor)
        clock = TestClock() if visitor.uses_test_stream else RealClock()

        # Performing configured PTransform overrides.
        original_overrides = direct_runner._get_transform_overrides(options)
        overrides = _update_overrides(original_overrides)
        pipeline.replace_all(overrides)

        _LOGGER.info("Running pipeline with Klio's GkeDirectRunner.")
        self.consumer_tracking_visitor = ConsumerTrackingPipelineVisitor()
        pipeline.visit(self.consumer_tracking_visitor)

        bundle_factory = BundleFactory(
            stacked=options.view_as(DirectOptions).direct_runner_use_stacked_bundle
        )
        evaluation_context = EvaluationContext(
            options,
            bundle_factory,
            self.consumer_tracking_visitor.root_transforms,
            self.consumer_tracking_visitor.value_to_consumers,
            self.consumer_tracking_visitor.step_names,
            self.consumer_tracking_visitor.views,
            clock)

        executor = Executor(
            self.consumer_tracking_visitor.value_to_consumers,
            utils.KlioTransformEvaluatorRegistry(evaluation_context),
            evaluation_context)
        # DirectRunner does not support injecting
        # PipelineOptions values at runtime
        RuntimeValueProvider.set_runtime_options({})
        # Start the executor. This is a non-blocking call, it will start the
        # execution in background threads and return.
        executor.start(self.consumer_tracking_visitor.root_transforms)
        result = direct_runner.DirectPipelineResult(executor, evaluation_context)

        return result

