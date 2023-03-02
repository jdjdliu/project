import datetime
import logbook
import sys
import traceback

from bigdata.common.utils import notify_by_email
from bigshared.common.service import MonitoredService
from bigdata.common.market import future_name_split
from bigdata.common.constants import FUTURE_INDEX_ID


# deprecated
class ServiceBase:
    def __init__(self, service_name, **kwargs):
        assert sys.version_info >= (3, 0)
        logbook.StreamHandler(stream=sys.stdout).push_application()
        self.service_name = service_name
        logbook.set_datetime_format("local")
        self.log = logbook.Logger(service_name)
        self.scheduled = kwargs.get('scheduled', False)
        self.notification_mail_to = kwargs.get('notification_mail_to', ['jliang@bigquant.com', 'hbweng@bigquant.com', 'yhzhao@bigquant.com', 'byzhang@bigquant.com'])

    def do_run(self):
        raise Exception('not implemented')

    def run(self):
        try:
            self.log.info('start')
            start_time = datetime.datetime.now()
            self.do_run()
            self.log.info('done in %ss' %
                     (datetime.datetime.now() - start_time).total_seconds())
            if self.scheduled:
                print('notifying %s ..' % self.notification_mail_to)
                msg = '<b>cmd</b>: %s\n\n<b>Last few logs</b>:\n%s' % ((sys.argv), '\n'.join(['#TODO']))
                notify_by_email(
                    self.notification_mail_to,
                    '【运行成功】%s 运行成功' % self.service_name,
                    msg.replace('\n', '<br/>'))
        except Exception as e:
            self.log.exception(e)
            if self.scheduled:
                msg = '<b>cmd</b>: %s\n\n' % (sys.argv)
                msg += str(traceback.format_exc()) + '\n\n'
                msg += '<b>Last few logs</b>: \n%s\n\n' % '\n'.join(['#TODO'])
                msg += 'More: check /etc/crontab and service logs for more info\n\n'
                print('notifying %s ..' % self.notification_mail_to)
                notify_by_email(
                    self.notification_mail_to,
                    '【严重问题】%s 运行失败' % self.service_name,
                    msg.replace('\n', '<br/>'))
            raise e


class BaseService(MonitoredService):
    def __init__(self, service_name, args=None):
        super().__init__(service_name, args)

    def init_arguments(self, parser):
        super().init_arguments(parser)
        parser.add_argument('--dev', action='store_true', help='开发模式，如果是开发模式，将只获取部分数据')

    def do_run(self):
        self.log.info('args, %s' % self.args)
        if self.args.func is not None:
            self.args.func()

    def _to_active_contract(self, instrument):
        code_type, code_id, market_name = future_name_split(instrument)
        return '%s.%s'%(code_type, market_name)

    def _to_index_contract(self, instrument):
        code_type, code_id, market_name = future_name_split(instrument)
        return '%s%s.%s'%(code_type, FUTURE_INDEX_ID, market_name)

