# -*- coding: utf-8 -*-
"""
    TRPC Implemented functions to create a server object
"""
import os
from typing import Set
from functools import partial

from trpc import config
from trpc import plugin
from trpc import utils
from trpc.filter import get_server_filter
from trpc.log import logger
from trpc.server import Server
from trpc.server import ServerOption
from trpc.server import Service
from trpc.server import ServiceOption
from trpc.transport import DefaultServerTransport
from trpc.transport import DefaultServerTransportOption
from trpc.transport import get_svr_transport
from trpc.plugin import check_new_trpc_naming_polaris

from .fork_hook import register_at_fork


def _run_service(server: Server):
    """ Select the location for running the service """
    for s in server.services.values():
        s.registry_service()


def new_service(service_name: str, service_option: ServiceOption) -> Service:
    """
    初始化单个service
    :param service_name: 服务名
    :param service_option: 服务名对应的配置
    :return : service对象
    """
    if service_option is None:
        service_option = ServiceOption()

    if service_option.transport is None:
        service_option.transport = DefaultServerTransport

    if service_option.transport_option is None:
        service_option.transport_option = DefaultServerTransportOption()

    service = Service(service_option=service_option)

    if not service.opt.handler_set:
        service.opt.set_handler(service.handle)

    service.opt.set_service_name(service_name)

    service_option.transport_option.set_service(service)

    return service


def save_bind_info(global_config: config.Config, service_config: config.Service):
    """
    将bind相关信息输出到指定路径下的文件中
    :param global_config: global配置
    :param service_config: service配置
    """
    if global_config.global_config.save_bind_info.enable:
        file_name = global_config.global_config.save_bind_info.file_name
        dirname = os.path.dirname(file_name)
        if dirname == '':
            dirname = './'
        os.makedirs(dirname, exist_ok=True)
        with open(file_name, 'a+') as f:
            f.write("%s %s %s\n" % (service_config.name, service_config.ip, service_config.port))


def new(conf_path: str, server_option: ServerOption = None, ignore_services: Set[str] = ()) -> Server:
    """
    初始化server
    :param conf_path: yaml文件的路径
    :param server_option: server配置，默认为空，从配置文件中读取
    :param ignore_services: 启动时可忽略指定的service，默认空表示所有service都启动
    :return : server对象
    """
    config.load_global_config(conf_path, 'utf-8')
    plugin.setup_master()

    if server_option is None:
        server_option = ServerOption()
    server_option.mode = config.global_config_obj.server.mode
    server_option.worker_num = config.global_config_obj.server.worker_num
    server_option.service_num = len(config.global_config_obj.server.service)
    server_option.task_wait_interval = config.global_config_obj.server.task_wait_interval
    server_option.task_batch_size = config.global_config_obj.server.task_batch_size
    server_option.task_queue_size = config.global_config_obj.server.task_queue_size
    server_option.task_consumer_num = config.global_config_obj.server.task_consumer_num
    server_option.reboot_child = config.global_config_obj.server.reboot_child
    server_option.close_wait_time = config.global_config_obj.server.close_wait_time
    server = Server(server_option)

    # admin服务
    for service_conf in config.global_config_obj.server.service:
        if service_conf.name in ignore_services:
            continue
        service_option = ServiceOption()
        service_option.app = server
        service_option.set_queue_wait_timeout(config.global_config_obj.server.queue_wait_timeout)
        service_option.set_queue_wait_policy(config.global_config_obj.server.queue_wait_policy)

        # registry
        if len(service_conf.registry) > 0:
            service_option.set_registry(service_conf.registry)

        # transport
        trans = get_svr_transport(service_conf.protocol)
        if trans is None:
            trans = DefaultServerTransport()
        service_option.transport = trans

        trans_option = None
        if trans_option is None:
            trans_option = DefaultServerTransportOption()
        trans_option.set_worker_num(config.global_config_obj.server.worker_num)
        trans_option.set_mode(config.global_config_obj.server.mode)
        trans_option.set_tls_key(service_conf.tls_key)
        trans_option.set_tls_cert(service_conf.tls_cert)
        trans_option.set_target(service_conf.target)
        service_option.transport_option = trans_option

        # base conf
        service_option.set_namespace(config.global_config_obj.global_config.namespace)
        service_option.set_env_name(config.global_config_obj.global_config.env_name)
        service_option.set_service_name(service_conf.name)
        service_option.set_protocol(service_conf.protocol)
        service_option.set_network(service_conf.network)
        service_option.set_timeout(int(service_conf.timeout))
        service_option.set_disable_request_timeout(service_conf.disable_request_timeout)
        service_option.set_enable_stream_filter(service_conf.enable_stream_filter)
        service_option.set_server_tls(service_conf.tls_cert, service_conf.tls_key)
        service_option.set_registry_delay(config.global_config_obj.server.registry_delay)
        service_option.set_registry_option(config.global_config_obj.server.registry_option)
        service_option.set_max_window_size(int(service_conf.max_window_size))
        if config.global_config_obj.global_config.enable_set == 'Y':
            service_option.set_setname(config.global_config_obj.global_config.full_set_name)
        service_option.set_socket_type(service_conf.socket_type)
        if service_conf.socket_type == config.SocketType.UNIX:
            service_option.set_address((service_conf.unix_path, 0))
        else:
            # 若port为空，则随机分配一个available port
            if service_conf.port == 0:
                service_conf.port = utils.find_free_port()
            service_option.set_address((service_conf.ip, service_conf.port))
            save_bind_info(config.global_config_obj, service_conf)

        # filter
        for filer_name in config.global_config_obj.server.filter:
            filter_ins = get_server_filter(filer_name)
            if filter_ins is None:
                raise Exception("Cannot find filter plugin for '%s'", filer_name)
            service_option.add_filter(filter_ins)
        for filer_name in service_conf.filter:
            filter_ins = get_server_filter(filer_name)
            if filter_ins is None:
                raise Exception("Cannot find filter plugin for '%s'", filer_name)
            service_option.add_filter(filter_ins)

        service = new_service(service_conf.name, service_option)
        server.add_service(service_conf.name, service)

    need_register = check_new_trpc_naming_polaris() or config.global_config_obj.server.registry_after_fork
    if need_register:
        register_at_fork(after_in_parent=partial(_run_service, server))
    else:
        register_at_fork(before=partial(_run_service, server))
    return server
