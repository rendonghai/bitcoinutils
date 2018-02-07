#
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class MailNotification(object):

    def __init__(self, server, me, pwd, recivers):
        self.smtp_server = smtplib.SMTP(server)
        self.me = me
        self.pwd = pwd
        self.recivers = recivers

    def send_notification(self, subject, content):

        msg = MIMEText(content, _subtype='plain')
        msg['Subject'] = subject
        msg['From'] = self.me
        msg['To'] = ';'.join(self.recivers)

        try:
            self.smtp_server.login(me, pwd)
            self.smtp_server.sendmail(self.me, msg['To'], msg.as_string())
        except Exception as e:
            print(e)
        finally:
            self.smtp_server.close()


if __name__ == '__main__':

    pass
