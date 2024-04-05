import logging
import sys
from exchangelib import ExtendedProperty, FaultTolerance, Configuration, Folder, FolderCollection
import os
from exchangelib.properties import DistinguishedFolderId
from psycopg2.pool import ThreadedConnectionPool as _ThreadedConnectionPool
from threading import Semaphore

em_logs = dict()

FOLDERS_IGNORE = [
    'Sync Issues', 
    'Junk Email', 
    'Deleted Items', 
    'GraphFilesAndWorkingSetSearchFolder',
    'People I Know',
    'RelevantContacts',
    'SharedFilesSearchFolder',
    'Favorites',
    'Sharing',
    'SpoolsPresentSharedItemsSearchFolder',
    'SpoolsSearchFolder',
    'UserCuratedContacts','My Contacts',
    'AllContacts',
    'AllContactsExtended',
    'AllPersonMetadata',
    'Favoritos',
    'Início do Repositório de Informações',
    'Meus Contatos',
    'PeopleConnect',
    'Recoverable Items',
    'Finder',
    'To-Do Search',
    'System',
    'AllCategorizedItems',
    'AllContacts',
    'AllContactsExtended',
    'AllItems',
    'AllPersonMetadata',
    'AllTodoTasks',
    'ApplicationDataRoot',
    'BrokerSubscriptions',
    'BulkActions',
    'Calendar Version Store',
    'CalendarItemSnapshots',
    'CalendarSearchCache',
    'CalendarSharingCacheCollection',
    'Common Views',
    'ComplianceMetadata',
    'Connectors',
    'CrawlerData',
    'DefaultFoldersChangeHistory',
    'Deferred Action',
    'DlpPolicyEvaluation',
    'Document Centric Conversations',
    'ExchangeODataSyncData',
    'ExchangeSyncData',
    'FileCollectionCache',
    'Folder Memberships',
    'Freebusy Data',
    'FreeBusyLocalCache',
    'GraphFilesAndWorkingSetSearchFolder',
    'GraphStore',
    'Inference',
    'Lembretes',
    'Location',
    'MailboxAssociations',
    'MeetingSapces',
    'MergedViewFolderCollection',
    'MessageIngestion',
    'MyAnalytics-ActionLog',
    'MyAnalytics-UnreadFromIRankerFolder',
    'MyAnalytics-UnreadFromVIPFolder',
    'O365 Suite Notifications',
    'O365 Suite Storage',
    'OneDriveRoot',
    'OneNotePagePreviews',
    'Orion Notes',
    'OutlookExtensions',
    'PACE',
    'ParkedMessages',
    'Pass-Through Search Results',
    'PdpProfile',
    'PdpProfileV2',
    'People I Know',
    'PeopleInsights',
    'PeoplePublicData',
    'QuarantinedEmail',
    'RecoveryPoints',
    'RelevantContacts',
    'Schedule',
    'SearchFoldersView',
    'ShadowItems',
    'ShardRelevancyFolder',
    'SharedFilesSearchFolder',
    'SharePointNotifications',
    'Sharing',
    'Shortcuts',
    'ShortNotes',
    'SkypeSpacesData',
    'SmsAndChatsSync',
    'SpamReports',
    'Spooler Queue',
    'SpoolsPresentSharedItemsSearchFolder',
    'SpoolsSearchFolder',
    'Subscriptions',
    'SubstrateFiles',
    'SuggestedUserGroupAssociations',
    'SwssItems',
    'TeamChatHistory',
    'TeamsMessagesData',
    'TemporarySaves',
    'UserCuratedContacts',
    'UserSocialActivityNotifications',
    'Views',
    'XrmActivityClientInstrumentation',
    'XrmActivityServerInstrumentation',
    'XrmActivityStream',
    'XrmActivityStreamSearch',
    'XrmCompanySearch',
    'XrmDealSearch',
    'XrmDeletedItems',
    'XrmInsights',
    'XrmProjects',
    'Itens Excluídos',
    'Lixo Eletrônico',
    'Problemas de Sincronização',
    'XrmSearch',
    'YammerData'
]

class EmThreadedConnectionPool(_ThreadedConnectionPool):
    def __init__(self, minconn, maxconn, *args, **kwargs):
        self._semaphore = Semaphore(maxconn)
        super().__init__(minconn, maxconn, *args, **kwargs)

    def getconn(self, *args, **kwargs):
        self._semaphore.acquire()
        try:
            return super().getconn(*args, **kwargs)
        except:
            self._semaphore.release()
            raise

    def putconn(self, *args, **kwargs):
        try:
            super().putconn(*args, **kwargs)
        finally:
            self._semaphore.release()

class CustomFieldSourceId(ExtendedProperty):
    distinguished_property_set_id = "Common"
    property_id = 0x00008688
    property_type = 'String'


class FolderMatch:

    def __init__(self, origin=None, dest=None):
        self.origin = origin
        self.dest = dest

class FolderDiscovery:
    def __init__(self, auto_create=True, logger=None):
        self.map_folders = {}
        self.auto_create = auto_create
        self.logger = logger

    def create_or_get_folder_dest(self, folder_name, parent_dest_folder):

        if parent_dest_folder is None:
            return None

        try:
            self.logger.info(f"Validando diretório: {parent_dest_folder.absolute} / {folder_name}")
            f = parent_dest_folder // folder_name
            return f
        except Exception as e:
            # Cria se não existir
            if self.auto_create:
                f = self.save_folder(folder_name, parent_dest_folder)
                return f
            else:
                return None

    def save_folder(self, folder_name, parent_dest_folder):
        new_folder = Folder(parent=parent_dest_folder, name=folder_name)
        new_folder = new_folder.save()
        return new_folder

    def equals_translated(self, folder1, folder2):
        if (folder1.lower() == 'inbox' and folder2.lower() == 'caixa de entrada') or \
            (folder1.lower() == 'caixa de entrada' and folder2.lower() == 'inbox'):
            return True
        
        if (folder1.lower() == 'outbox' and folder2.lower() == 'caixa de saída') or \
            (folder1.lower() == 'caixa de saída' and folder2.lower() == 'outbox'):
            return True
        
        if (folder1.lower() == 'sent items' and folder2.lower() == 'itens enviados') or \
            (folder1.lower() == 'itens enviados' and folder2.lower() == 'sent items'):
            return True

        return folder1 == folder2

    def traverse_and_create(self, folder, parent_dest_folder):
        if parent_dest_folder is not None and self.equals_translated(folder.name, parent_dest_folder.name):
            new_dest_folder = parent_dest_folder
        else:
            new_dest_folder = self.create_or_get_folder_dest(folder.name, parent_dest_folder)

        if folder.id not in self.map_folders:
            self.map_folders[folder.id] = FolderMatch(origin=folder, dest=new_dest_folder)

        for subfolder in folder.children:
            #Diretório de emails
            if (subfolder.folder_class == 'IPF.Note' or subfolder.name == 'Top of Information Store' or subfolder.name == 'Início do Repositório de Informações') and subfolder.name not in FOLDERS_IGNORE:
                self.traverse_and_create(subfolder, new_dest_folder)

    def add_contacts_folder(self, acc_orig, acc_dest):
        folder_ori = acc_orig.contacts
        
        if acc_dest is None:
            folder_dest = None
        else:
            folder_dest = acc_dest.contacts

        self.map_folders[folder_ori.id] = FolderMatch(origin=folder_ori, dest=folder_dest)

    def add_calendars_folder(self, acc_orig, acc_dest):

        folder_ori = acc_orig.calendar

        if acc_dest is None:
            folder_dest = None
        else:
            folder_dest = acc_dest.calendar

        self.map_folders[folder_ori.id] = FolderMatch(origin=folder_ori, dest=folder_dest)

    def add_messages_folder(self, acc_orig, acc_dest):

        inbox_dest = acc_dest.inbox if acc_dest is not None else None
        outbox_dest = acc_dest.outbox if acc_dest is not None else None
        sent_dest = acc_dest.sent if acc_dest is not None else None
        drafts_dest = acc_dest.drafts if acc_dest is not None else None

        self.map_folders[acc_orig.inbox.id] = FolderMatch(origin=acc_orig.inbox, dest=inbox_dest)
        self.map_folders[acc_orig.outbox.id] = FolderMatch(origin=acc_orig.outbox, dest=outbox_dest)
        self.map_folders[acc_orig.sent.id] = FolderMatch(origin=acc_orig.sent, dest=sent_dest)
        self.map_folders[acc_orig.drafts.id] = FolderMatch(origin=acc_orig.drafts, dest=drafts_dest)

    def add_first_level(self, acc_orig, acc_dest):

        root_folder_orig = acc_orig.msg_folder_root

        if acc_dest is not None:
            root_folder_dest = acc_dest.msg_folder_root
        else:
            root_folder_dest = None

        for subfolder in root_folder_orig.children:

            if subfolder.id not in self.map_folders:
                if (subfolder.folder_class == 'IPF.Note') and subfolder.name not in FOLDERS_IGNORE:
                    self.traverse_and_create(subfolder, root_folder_dest)


def getLogger(name):

    name = name.replace('.', '_').replace('@', '_')

    if name in em_logs.keys():
        return em_logs[name]

    logger = logging.Logger(name)
    logger.setLevel(logging.DEBUG)
    
    time_format = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(message)s', datefmt=time_format)


    if not os.path.exists('logs'):
        os.makedirs('logs')

    handler = logging.FileHandler(os.path.join('logs/', name + '.log'), 'a')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    handler = logging.FileHandler(os.path.join('logs/complete.log'), 'a')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    em_logs[name] = logger

    return logger
