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
"""
Provides an interface to fake control plane server running remotely.
"""
import functools
import logging
from typing import Iterator, Optional
from enum import Enum

from grpc import StatusCode
import framework.rpc
from framework.rpc import grpc_channelz
from framework.rpc import grpc_testing

logger = logging.getLogger(__name__)

# Type aliases
_ChannelzServiceClient = grpc_channelz.ChannelzServiceClient
_XdsUpdateHealthServiceClient = grpc_testing.XdsUpdateHealthServiceClient
_HealthClient = grpc_testing.HealthClient


class FakeXdsControlPlaneServer(framework.rpc.grpc.GrpcApp):
  """
    Provides a controlled alternative to Traffic Director to let tests verify
    control plane error conditions.
    """

  def __init__(self,
               *,
               ip: str,
               rpc_port: int,
               hostname: str,
               maintenance_port: Optional[int] = None,
               secure_mode: Optional[bool] = False,
               xds_host: Optional[str] = None,
               xds_port: Optional[int] = None,
               rpc_host: Optional[str] = None):
    super().__init__(rpc_host=(rpc_host or ip))
    self.ip = ip
    self.rpc_port = rpc_port
    self.hostname = hostname
    self.maintenance_port = maintenance_port or rpc_port
    self.secure_mode = secure_mode
    self.xds_host, self.xds_port = xds_host, xds_port

  @property
  @functools.lru_cache(None)
  def channelz(self) -> _ChannelzServiceClient:
    return _ChannelzServiceClient(
      self._make_channel(self.maintenance_port),
      log_target=f'{self.hostname}:{self.maintenance_port}')

  @property
  @functools.lru_cache(None)
  def update_health_service_client(self) -> _XdsUpdateHealthServiceClient:
    return _XdsUpdateHealthServiceClient(
      self._make_channel(self.maintenance_port),
      log_target=f'{self.hostname}:{self.maintenance_port}')

  @property
  @functools.lru_cache(None)
  def health_client(self) -> _HealthClient:
    return _HealthClient(
      self._make_channel(self.maintenance_port),
      log_target=f'{self.hostname}:{self.maintenance_port}')

  def set_xds_address(self, xds_host, xds_port: Optional[int] = None):
    self.xds_host, self.xds_port = xds_host, xds_port

  @property
  def xds_address(self) -> str:
    if not self.xds_host:
      return ''
    if not self.xds_port:
      return self.xds_host
    return f'{self.xds_host}:{self.xds_port}'

  @property
  def xds_uri(self) -> str:
    if not self.xds_host:
      return ''
    return f'xds:///{self.xds_address}'

  def get_test_server(self) -> grpc_channelz.Server:
    """Return channelz representation of a server running TestService.

        Raises:
            GrpcApp.NotFound: Test server not found.
        """
    server = self.channelz.find_server_listening_on_port(self.rpc_port)
    if not server:
      raise self.NotFound(f'[{self.hostname}] Server'
                          f'listening on port {self.rpc_port} not found')
    return server

  def get_test_server_sockets(self) -> Iterator[grpc_channelz.Socket]:
    """List all sockets of the test server.

        Raises:
            GrpcApp.NotFound: Test server not found.
        """
    server = self.get_test_server()
    return self.channelz.list_server_sockets(server)

  # def get_server_socket_matching_client(self,
  #                                       client_socket: grpc_channelz.Socket):
  #     """Find test server socket that matches given test client socket.
  #
  #     Sockets are matched using TCP endpoints (ip:port), further on "address".
  #     Server socket remote address matched with client socket local address.
  #
  #      Raises:
  #          GrpcApp.NotFound: Server socket matching client socket not found.
  #      """
  #     client_local = self.channelz.sock_address_to_str(client_socket.local)
  #     logger.debug(
  #         '[%s] Looking for a server socket connected '
  #         'to the client %s', self.hostname, client_local)
  #
  #     server_socket = self.channelz.find_server_socket_matching_client(
  #         self.get_test_server_sockets(), client_socket)
  #     if not server_socket:
  #         raise self.NotFound(f'[{self.hostname}] Socket '
  #                             f'to client {client_local} not found')
  #
  #     logger.info(
  #         '[%s] Found matching socket pair: '
  #         'server(%s) <-> client(%s)', self.hostname,
  #         self.channelz.sock_addresses_pretty(server_socket),
  #         self.channelz.sock_addresses_pretty(client_socket))
  #     return server_socket


class TriggerTime(Enum):
  BEFORE_LDS = 1
  BEFORE_CDS = 2
  BEFORE_RDS = 3
  BEFORE_ENDPOINTS = 4
  AFTER_ENDPOINTS = 5


class ExtraResourceChoice(Enum):
  EXTRA_ONLY = 1
  ALL_REQ = 2
  SOME_REQ = 3  # replace as many required as there are extra so total number is what was expected


class ResourceType(Enum):
  LDS = 'lds'
  CDS = 'cds'
  RDS = 'rds'
  EDS = 'eds'


class AberationType(Enum):
  STATUS_CODE = 'status code'
  SEND_EXTRA = 'send extra'
  SEND_EMPTY = 'send empty'
  SEND_REDUNDANT = 'send redundant'
  MISSING_RESOURCES = 'skip setting'


class ControlData:

  def __init__(self,
               trigger_aberration: TriggerTime,
               aberration_type: AberationType = None,
               status_code=StatusCode.OK,
               affected_types: [ResourceType] = []
               ):
    self.trigger_aberration = trigger_aberration
    self.status_code = status_code
    self.affected_types = affected_types
    self.aberration_type = aberration_type
