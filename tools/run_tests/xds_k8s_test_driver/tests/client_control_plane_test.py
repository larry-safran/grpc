# Copyright 2022 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging

from absl import flags
from absl.testing import absltest

from grpc import StatusCode
from framework import xds_k8s_flags
from framework import xds_k8s_testcase
from framework.helpers import skips
from framework.infrastructure import k8s
from framework.test_app import fake_xds_control_plan_app
from framework.test_app.runners.k8s import k8s_xds_server_runner

from tools.run_tests.xds_k8s_test_driver.framework.helpers import datetime
from tools.run_tests.xds_k8s_test_driver.framework.test_app.fake_xds_control_plane_app import \
  ControlData, AberationType

logger = logging.getLogger(__name__)
flags.adopt_module_key_flags(xds_k8s_testcase)

# Type aliases
_Lang = skips.Lang
_XdsTestServer = xds_k8s_testcase.XdsTestServer
_XdsTestClient = xds_k8s_testcase.XdsTestClient
_KubernetesServerRunner = k8s_xds_server_runner.KubernetesFakeXdsControlPlaneRunner
_TriggerTime = fake_xds_control_plan_app.TriggerTime
_ExtraResourceChoice = fake_xds_control_plan_app.ExtraResourceChoice
_ResourceType = fake_xds_control_plan_app.ResourceType
_TypedTrigger = fake_xds_control_plan_app.TypedTrigger


class ClientCPTest(xds_k8s_testcase.FakeControlPlaneXdsKubernetesTestCase):
  REPLICA_COUNT = 3
  MAX_RATE_PER_ENDPOINT = 100

  STANDARD_LDS_CONFIG = ''  # TODO figure out actual
  STANDARD_RDS_CONFIG = ''  # TODO figure out actual
  STANDARD_CDS_CONFIG = ''  # TODO figure out actual
  STANDARD_EDS_CONFIG = ''  # TODO figure out actual

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    # Force the python client to use the reference server image (Java)
    # because the python server doesn't yet support set_not_serving RPC.
    # TODO(https://github.com/grpc/grpc/issues/30635): Remove when resolved.
    if cls.lang_spec.client_lang == _Lang.PYTHON:
      cls.server_image = xds_k8s_flags.SERVER_IMAGE_CANONICAL.value

  def setUp(self):
    super().setUp()
    self.secondary_server_runner = _KubernetesServerRunner(
      k8s.KubernetesNamespace(self.secondary_k8s_api_manager,
                              self.server_namespace),
      deployment_name=self.server_name + '-alt',
      image_name=self.server_image,
      gcp_service_account=self.gcp_service_account,
      td_bootstrap_image=self.td_bootstrap_image,
      gcp_project=self.project,
      gcp_api_manager=self.gcp_api_manager,
      xds_server_uri=self.xds_server_uri,
      network=self.network,
      debug_use_port_forwarding=self.debug_use_port_forwarding,
      force_cleanup=True,
      # This runner's namespace created in the secondary cluster,
      # so it's not reused and must be cleaned up.
      reuse_namespace=False)
    # Setup configuration
    self.setStandardXdsConfig()

  def cleanup(self):
    super().cleanup()
    self.secondary_server_runner.cleanup(
      force=self.force_cleanup, force_namespace=self.force_cleanup)

  def test_client_cp(self) -> None:
    # start server

    with self.subTest('00_smoke_test'):
      self.smokeTest()

    with self.subTest('01_status_codes'):
      self.checkStatusCodes()

    with self.subTest('02_unexpected_resources'):
      self.checkUnexpectedResources()

    with self.subTest('03_empty_updates'):
      self.checkEmptyUpdates()

    with self.subTest('04_redundant_updates'):
      self.checkRedundantUpdates()

    with self.subTest('05_missing_required_resources'):
      self.checkMissingRes()

    with self.subTest('06_delete_resources'):
      self.checkDeleteRes()

############## Sub tests

  def smokeTest(self):
    # TODO start client
    self.assertSuccessfulRpcs(self.test_client)
    # TODO cleanup client & reset stats

  def checkStatusCodes(self) -> None:
    for trigger in [_TriggerTime.BEFORE_LDS,
                    _TriggerTime.BEFORE_ENDPOINTS,
                    _TriggerTime.AFTER_ENDPOINTS]:
      for code in StatusCode.__members__.items():
        if (code != StatusCode.OK):
          self.assertStatusCode(trigger, code)

  def checkUnexpectedResources(self) -> None:
    extra_resources = ''  # TODO define extra resources
    self.cp.setExtraResources(extra_resources)
    self.testDuringProcess(AberationType.SEND_EXTRA)

  def checkEmptyUpdates(self) -> None:
    self.testDuringProcess(AberationType.SEND_EMPTY)

  def checkRedundantUpdates(self) -> None:
    self.testDuringProcess(AberationType.SEND_REDUNDANT)

  def checkMissingRes(self) -> None:
    self.test_client = self.createClient()
    for trigger_time in [_TriggerTime.BEFORE_RDS,
                         _TriggerTime.BEFORE_CDS,
                         _TriggerTime.BEFORE_ENDPOINTS]:
      self.sendResourcesUntil(trigger_time)
      self.assertRpcsUnavailable(self.test_client)

    # Verify everything starts working when full configuration is sent
    self.cp.updateControlData(ControlData(None, None))
    self.assertSuccessfulRpcs(self.test_client)


  def checkDeleteRes(self) -> None:
    self.testDeleteBeforeEndpoints('LDS', self.STANDARD_LDS_CONFIG)
    self.testDeleteBeforeEndpoints('CDS', self.STANDARD_CDS_CONFIG)

    self.testDeleteAfterEndpoints('LDS', self.STANDARD_LDS_CONFIG)
    self.testDeleteAfterEndpoints('CDS', self.STANDARD_CDS_CONFIG)


######### Helper functions

  def setStandardXdsConfig(self, include_eds=True):
    self.cp.setXdsConfig('LDS', self.STANDARD_LDS_CONFIG)  # todo get actual config
    self.cp.setXdsConfig('CDS', self.STANDARD_CDS_CONFIG)
    self.cp.setXdsConfig('RDS', self.STANDARD_RDS_CONFIG)
    if (include_eds):
      self.cp.setXdsConfig('EDS', self.STANDARD_EDS_CONFIG)
    else:
      self.cp.setXdsConfig('EDS', None)

    self.cp.setExtraResources(None)

  def assertStatusCode(self, trigger_aberration: _TriggerTime, status_code: StatusCode):
    self.cp.updateControlData(ControlData(trigger_aberration,
                                          AberationType.STATUS_CODE,
                                          status_code))
    self.test_client = self.createClient()
    self.assertRpcStatusCodes(self.test_client,
                              status_code=status_code,
                              duration=datetime.timedelta(seconds=10),
                              method='UNARY_CALL')

  def testDuringProcess(self, ab_type: AberationType):
    cd = ControlData(_TriggerTime.BEFORE_LDS, ab_type)

    self.cp.updateControlData(cd)
    self.test_client = self.createClient()
    # TODO check for what?

    cd.trigger_aberration = _TriggerTime.BEFORE_ENDPOINTS
    self.cp.updateControlData(cd)
    self.test_client = self.createClient()
    # TODO check that RPCs are queued

    cd.trigger_aberration = _TriggerTime.AFTER_ENDPOINTS
    self.cp.updateControlData(cd)
    self.test_client = self.createClient()
    self.assertSuccessfulRpcs(self.test_client)

  def sendResourcesUntil(self, trigger_time: _TriggerTime):
    cd = ControlData(trigger_time, AberationType.MISSING_RESOURCES,)
    self.cp.updateControlData(cd)

  def testDeleteBeforeEndpoints(self, ds_type:str, default:str):
    self.sendResourcesUntil(_TriggerTime.BEFORE_ENDPOINTS)
    self.test_client = self.createClient()

    self.cp.setXdsConfig(ds_type, None)
    self.cp.updateControlData(ControlData(None))
    self.assertRpcsUnavailable(self.test_client)

    # restore the default and it should succeed
    self.cp.setXdsConfig(ds_type, default)
    self.assertSuccessfulRpcs(self.test_client)

  def testDeleteAfterEndpoints(self, ds_type:str, default:str):
    new_style = False  # With new option set in the bootstrap these would succeed
    self.setStandardXdsConfig()
    self.test_client = self.createClient()
    self.cp.setXdsConfig(ds_type, None)
    self.cp.updateControlData(ControlData(None))

    if (new_style):
      self.assertSuccessfulRpcs(self.test_client)
    else:
      self.assertRpcsUnavailable(self.test_client)

    # restore the default and it should succeed
    self.cp.setXdsConfig(ds_type, default)
    self.assertSuccessfulRpcs(self.test_client)

  def assertRpcsUnavailable(self, test_client):
    self.assertRpcStatusCodes(test_client,
                              status_code=StatusCode.UNAVAILABLE,
                              duration=datetime.timedelta(seconds=10),
                              method='UNARY_CALL')

if __name__ == '__main__':
  absltest.main(failfast=True)
