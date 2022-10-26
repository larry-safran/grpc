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

import grpc
from absl import flags
from absl.testing import absltest

from grpc import StatusCode
from framework import xds_k8s_flags
from framework import xds_k8s_testcase
from framework.helpers import skips
from framework.test_app.runners.k8s import k8s_xds_fake_control_plane_runner

from framework.helpers import datetime
from src.proto.grpc.testing.xds.v3.xds_test_config_service_pb2 import AberrationType, ControlData,\
  TriggerTime
import src.proto.grpc.testing.xds.v3.xds_test_config_service_pb2_grpc as gcfg_svc


logger = logging.getLogger(__name__)
flags.adopt_module_key_flags(xds_k8s_testcase)

# Type aliases
_Lang = skips.Lang
_XdsTestServer = xds_k8s_testcase.XdsTestServer
_XdsTestClient = xds_k8s_testcase.XdsTestClient
K8sCpRunner = k8s_xds_fake_control_plane_runner.KubernetesFakeXdsControlPlaneRunner


class ClientCPTest(xds_k8s_testcase.FakeControlPlaneXdsKubernetesTestCase):
  REPLICA_COUNT = 1
  MAX_RATE_PER_ENDPOINT = 100
  cp: gcfg_svc.XdsTestConfigService

  EXTRA_CONFIG = '''
  {
    "@type": "type.googleapis.com/grpc.testing.ExtraResourceRequest",
    "configurations": [
      {
        "@type": "type.googleapis.com/grpc.testing.XdsConfig",
        "type" : 0,
        "configuration": [ {
          "@type": "type.googleapis.com/envoy.config.listener.v3.Listener",
          "name": "dummy1",
        }]
      },
      {
        "@type": "type.googleapis.com/envoy.config.route.v3.RouteConfiguration",
        "name": "dummy1R",
      },
      {
        "@type": "type.googleapis.com/envoy.config.cluster.v3.Cluster",
        "name": "dummy1C",
      }
    ]
  }
  '''

  STANDARD_LDS_CONFIG = '''
  {
    "@type": "type.googleapis.com/envoy.config.listener.v3.Listener",
    "name": "psm-grpc-server:14427",
    "apiListener": {
      "apiListener": {
        "@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
        "statPrefix": "trafficdirector",
        "rds": {
          "configSource": {
            "ads": {
            },
            "resourceApiVersion": "V3"
          },
          "routeConfigName": "URL_MAP/830293263384_psm-interop-url-map-20220928-0836-dz2xb_0_psm-grpc-server:14427"
        },
        "httpFilters": [{
          "name": "envoy.filters.http.fault",
          "typedConfig": {
            "@type": "type.googleapis.com/envoy.extensions.filters.http.fault.v3.HTTPFault"
          }
        }, {
          "name": "envoy.filters.http.router",
          "typedConfig": {
            "@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router",
            "suppressEnvoyHeaders": true
          }
        }],
        "normalizePath": true,
        "mergeSlashes": true
      }
    }
  }
  '''

  STANDARD_RDS_CONFIG = '''
  {
    "@type": "type.googleapis.com/envoy.config.route.v3.RouteConfiguration",
    "name": "URL_MAP/830293263384_psm-interop-url-map-20220928-0836-dz2xb_0_psm-grpc-server:14427",
    "virtualHosts": [{
      "domains": ["psm-grpc-server:14427"],
      "routes": [{
        "match": {
          "prefix": ""
        },
        "route": {
          "cluster": "cloud-internal-istio:cloud_mp_830293263384_5089449662400280953",
          "timeout": "30s",
          "retryPolicy": {
            "retryOn": "gateway-error",
            "numRetries": 1,
            "perTryTimeout": "30s"
          }
        },
        "name": "URL_MAP/830293263384_psm-interop-url-map-20220928-0836-dz2xb_0_psm-grpc-server:14427-route-0"
      }]
    }]
  }
    '''  # TODO figure out actual
  STANDARD_CDS_CONFIG = '''
  {
    "@type": "type.googleapis.com/envoy.config.cluster.v3.Cluster",
    "name": "cloud-internal-istio:cloud_mp_830293263384_5089449662400280953",
    "type": "EDS",
    "edsClusterConfig": {
      "edsConfig": {
        "ads": {
        },
        "initialFetchTimeout": "15s",
        "resourceApiVersion": "V3"
      }
    },
    "connectTimeout": "30s",
    "circuitBreakers": {
      "thresholds": [{
        "maxConnections": 2147483647,
        "maxPendingRequests": 2147483647,
        "maxRequests": 2147483647,
        "maxRetries": 2147483647
      }]
    },
    "http2ProtocolOptions": {
      "maxConcurrentStreams": 100
    },
    "transportSocket": {
      "name": "envoy.transport_sockets.tls",
      "typedConfig": {
        "@type": "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext",
        "commonTlsContext": {
          "alpnProtocols": ["h2"],
          "tlsCertificateSdsSecretConfigs": [{
            "name": "tls_sds",
            "sdsConfig": {
              "path": "/etc/envoy/tls_certificate_context_sds_secret.yaml"
            }
          }],
          "combinedValidationContext": {
            "defaultValidationContext": {
              "matchSubjectAltNames": [{
                "exact": "spiffe://grpc-testing.svc.id.goog/ns/psm-interop-server-20220928-0836-dz2xb/sa/psm-grpc-server"
              }]
            },
            "validationContextSdsSecretConfig": {
              "name": "validation_context_sds",
              "sdsConfig": {
                "path": "/etc/envoy/validation_context_sds_secret.yaml"
              }
            },
            "validationContextCertificateProviderInstance": {
              "instanceName": "google_cloud_private_spiffe",
              "certificateName": "ROOTCA"
            }
          },
          "tlsCertificateCertificateProviderInstance": {
            "instanceName": "google_cloud_private_spiffe",
            "certificateName": "DEFAULT"
          }
        }
      }
    },
    "metadata": {
      "filterMetadata": {
        "com.google.trafficdirector": {
          "backend_service_name": "psm-interop-backend-service-20220928-0836-dz2xb",
          "backend_service_project_number": 8.30293263384E11
        }
      }
    },
    "commonLbConfig": {
      "healthyPanicThreshold": {
        "value": 1.0
      },
      "localityWeightedLbConfig": {
      }
    },
    "altStatName": "/projects/830293263384/global/backendServices/psm-interop-backend-service-20220928-0836-dz2xb",
    "lrsServer": {
      "self": {
      }
    }
  }
  '''  # TODO figure out actual

  STANDARD_EDS_CONFIG = '''
  {
    "@type": "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment",
    "clusterName": "cloud-internal-istio:cloud_mp_830293263384_5089449662400280953",
    "endpoints": [{
      "locality": {
        "subZone": "ib:us-central1-a_2239524602765751982_neg"
      },
      "lbEndpoints": [{
        "endpoint": {
          "address": {
            "socketAddress": {
              "address": "10.88.5.48",
              "portValue": 8080
            }
          }
        },
        "healthStatus": "HEALTHY"
      }],
      "loadBalancingWeight": 100
    }]
  }
  '''  # TODO figure out actual

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

    # Launch fake control plane
    # self.cp_namespace = KubernetesServerRunner.make_namespace_name(
    #   self.resource_prefix, self.resource_suffix)
    self.cp_runner = self.initFakeXdsControlPlane()
    self.cp = self.startFakeControlPlane(self.cp_runner)
    with grpc.insecure_channel('localhost:50051') as channel:
      self.control_stub = gcfg_svc.XdsTestConfigServiceStub(channel)

    test_server: _XdsTestServer = self.startTestServers()[0]
    # self.setupServerBackends()
    self.setStandardXdsConfig()
    test_client: _XdsTestClient = self.startTestClient(test_server)

  def cleanup(self):
    super().cleanup()
    self.cp_runner.cleanup(force=self.force_cleanup)

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
    for trigger in [TriggerTime.BEFORE_LDS,
                    TriggerTime.BEFORE_ENDPOINTS,
                    TriggerTime.AFTER_ENDPOINTS]:
      for code in StatusCode.__members__.items():
        if code != StatusCode.OK:
          self.assertStatusCode(trigger, code)

  def checkUnexpectedResources(self) -> None:
    self.control_stub.setExtraResources(self.EXTRA_CONFIG)
    self.testDuringProcess(AberrationType.SEND_EXTRA)

  def checkEmptyUpdates(self) -> None:
    self.testDuringProcess(AberrationType.SEND_EMPTY)

  def checkRedundantUpdates(self) -> None:
    self.testDuringProcess(AberrationType.SEND_REDUNDANT)

  def checkMissingRes(self) -> None:
    self.test_client = self.createClient()
    for trigger_time in [TriggerTime.BEFORE_RDS,
                         TriggerTime.BEFORE_CDS,
                         TriggerTime.BEFORE_ENDPOINTS]:
      self.sendResourcesUntil(trigger_time)
      self.assertRpcsUnavailable(self.test_client)

    # Verify everything starts working when full configuration is sent
    self.control_stub.updateControlData(ControlData(None, None))
    self.assertSuccessfulRpcs(self.test_client)


  def checkDeleteRes(self) -> None:
    self.testDeleteBeforeEndpoints('LDS', self.STANDARD_LDS_CONFIG)
    self.testDeleteBeforeEndpoints('CDS', self.STANDARD_CDS_CONFIG)

    self.testDeleteAfterEndpoints('LDS', self.STANDARD_LDS_CONFIG)
    self.testDeleteAfterEndpoints('CDS', self.STANDARD_CDS_CONFIG)


######### Helper functions

  def setStandardXdsConfig(self, include_eds=True):
    # TODO massage standard configs using server host and port

    self.control_stub.setXdsConfig('LDS', self.STANDARD_LDS_CONFIG)
    self.control_stub.setXdsConfig('CDS', self.STANDARD_CDS_CONFIG)
    self.control_stub.setXdsConfig('RDS', self.STANDARD_RDS_CONFIG)
    if include_eds:
      self.control_stub.setXdsConfig('EDS', self.STANDARD_EDS_CONFIG)
    else:
      self.control_stub.setXdsConfig('EDS', None)

    self.control_stub.setExtraResources(None)

  def assertStatusCode(self, trigger_aberration: TriggerTime, status_code: StatusCode):
    self.control_stub.updateControlData(ControlData(trigger_aberration,
                                          AberrationType.STATUS_CODE,
                                          status_code))
    self.test_client = self.createClient()
    self.assertRpcStatusCodes(self.test_client,
                              status_code=status_code,
                              duration=datetime.timedelta(seconds=10),
                              method='UNARY_CALL')

  def testDuringProcess(self, ab_type: AberrationType):
    cd = ControlData(TriggerTime.BEFORE_LDS, ab_type)

    self.control_stub.updateControlData(cd)
    self.test_client = self.createClient()
    # TODO check for what?

    cd.trigger_aberration = TriggerTime.BEFORE_ENDPOINTS
    self.control_stub.updateControlData(cd)
    self.test_client = self.createClient()
    # TODO check that RPCs are queued

    cd.trigger_aberration = TriggerTime.AFTER_ENDPOINTS
    self.control_stub.updateControlData(cd)
    self.test_client = self.createClient()
    self.assertSuccessfulRpcs(self.test_client)

  def sendResourcesUntil(self, trigger_time: TriggerTime):
    cd = ControlData(trigger_time, AberrationType.MISSING_RESOURCES,)
    self.control_stub.updateControlData(cd)

  def testDeleteBeforeEndpoints(self, ds_type:str, default:str):
    self.sendResourcesUntil(TriggerTime.BEFORE_ENDPOINTS)
    self.test_client = self.createClient()

    self.control_stub.setXdsConfig(ds_type, None)
    self.control_stub.updateControlData(ControlData(None))
    self.assertRpcsUnavailable(self.test_client)

    # restore the default and it should succeed
    self.control_stub.setXdsConfig(ds_type, default)
    self.assertSuccessfulRpcs(self.test_client)

  def testDeleteAfterEndpoints(self, ds_type:str, default:str):
    new_style = False  # With new option set in the bootstrap these would succeed
    self.setStandardXdsConfig()
    self.test_client = self.createClient()
    self.control_stub.setXdsConfig(ds_type, None)
    self.control_stub.updateControlData(ControlData(None))

    if new_style:
      self.assertSuccessfulRpcs(self.test_client)
    else:
      self.assertRpcsUnavailable(self.test_client)

    # restore the default and it should succeed
    self.control_stub.setXdsConfig(ds_type, default)
    self.assertSuccessfulRpcs(self.test_client)

  def assertRpcsUnavailable(self, test_client):
    self.assertRpcStatusCodes(test_client,
                              status_code=StatusCode.UNAVAILABLE,
                              duration=datetime.timedelta(seconds=10),
                              method='UNARY_CALL')


if __name__ == '__main__':
  absltest.main(failfast=True)
