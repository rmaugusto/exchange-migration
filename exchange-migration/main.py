from exchangelib import CalendarItem, Folder, Message
from exchangelib.items import (
    Message,
    Contact,  MeetingRequest, MeetingCancellation
)
from utils import CustomFieldSourceId
from yaml import load, Loader
from email_stats import EmailStats
from emails import EmailMigrator

def main():

    Contact.register('source_id', CustomFieldSourceId)
    CalendarItem.register('source_id', CustomFieldSourceId)
    Message.register('source_id', CustomFieldSourceId)
    MeetingRequest.register('source_id', CustomFieldSourceId)
    MeetingCancellation.register('source_id', CustomFieldSourceId)

    config = load(open('config.yaml', 'r'), Loader=Loader)

    accounts = config['accounts']

    for account_idx in range(0, len(accounts)):
        em = EmailMigrator()
        #em = EmailStats()
        em.run(config, account_idx)

if __name__ == "__main__":
    main()
