#
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class MailNotification(object):

    def __init__(self, server, me, pwd, recivers):
        self.server = server
        self.me = me
        self.pwd = pwd
        self.recivers = recivers

    def send_notification(self, subject, content):

        msg = MIMEText(content, _subtype='plain')
        msg['Subject'] = subject
        msg['From'] = self.me
        msg['To'] = ', '.join(self.recivers)

        smtp_server = smtplib.SMTP(self.server)
        try:
            smtp_server.login(self.me, self.pwd)
            smtp_server.sendmail(self.me, self.recivers, msg.as_string())
        except Exception as e:
            print(e)
        finally:
            smtp_server.close()


if __name__ == '__main__':

    notifier = MailNotification('smtp.mxhichina.com' , 'notice@hkhongyi.net', 'Rdh11223344@', ['rdh_wx@163.com', 'wxjeacen@gmail.com'])
    #notifier = MailNotification('smtp.mxhichina.com' , 'notice@hkhongyi.net', 'Rdh11223344@', ['wxjeacen@gmail.com'])
    notifier.send_notification('This is a test', 'Test ok')
    import time
    time.sleep(5)
    notifier.send_notification('This is a second test', 'Test ok')
    pass
