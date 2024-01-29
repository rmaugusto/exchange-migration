import logging
import time
from exchangelib import FaultTolerance, Configuration
from exchangelib import IMPERSONATION, Account, CalendarItem, ExtendedProperty, Folder, Message, OAuth2Credentials
from exchangelib.items import (
    Message,
    Contact, DistributionList, Persona, MeetingRequest, MeetingCancellation
)
from exchangelib import Q
from thread import ThreadPool
import os


#logging.basicConfig(level=logging.DEBUG, handlers=[PrettyXmlHandler()])

FOLDERS_IGNORE = ['Sync Issues', 'Junk Email', 'Deleted Items', 'GraphFilesAndWorkingSetSearchFolder',
'People I Know',
'RelevantContacts',
'SharedFilesSearchFolder',
'Sharing',
'SpoolsPresentSharedItemsSearchFolder',
'SpoolsSearchFolder',
'UserCuratedContacts','My Contacts',
'AllContacts',
'AllContactsExtended',
'AllPersonMetadata',]

def getLogger(name):
    name = name.replace('.', '_').replace('@', '_')
    logger = logging.Logger(name)
    logger.setLevel(logging.DEBUG)

    if not os.path.exists('logs'):
        os.makedirs('logs')

    handler = logging.FileHandler(os.path.join('logs/', name + '.log'), 'a')
    logger.addHandler(handler)
    return logger

class CustomFieldSourceId(ExtendedProperty):
    distinguished_property_set_id = "Common"
    property_id = 0x00008525
    property_type = 'String'

Contact.register('source_id', CustomFieldSourceId)
CalendarItem.register('source_id', CustomFieldSourceId)
Message.register('source_id', CustomFieldSourceId)
MeetingRequest.register('source_id', CustomFieldSourceId)
MeetingCancellation.register('source_id', CustomFieldSourceId)

class FolderMatch:

    def __init__(self, origin=None, dest=None):
        self.origin = origin
        self.dest = dest

class ExchangeFolderMigrator:
    def __init__(self):
        self.map_folders = {}

    def create_or_get_folder(self, folder_name, parent_dest_folder):
        try:
            #print("Validando diretório: ", parent_dest_folder.absolute + '/' + folder_name)
            f = parent_dest_folder / folder_name
            return f
        except Exception as e:
            # Cria se não existir
            f = self.save_folder(folder_name, parent_dest_folder)
            return f

    def save_folder(self, folder_name, parent_dest_folder):
        new_folder = Folder(parent=parent_dest_folder, name=folder_name)
        new_folder = new_folder.save()
        #print("Criado diretório: ", new_folder.absolute)
        return new_folder

    def traverse_and_create(self, folder, parent_dest_folder):
        if folder.name != parent_dest_folder.name:  # Evita duplicar root
            new_dest_folder = self.create_or_get_folder(folder.name, parent_dest_folder)
        else:
            new_dest_folder = parent_dest_folder

        self.map_folders[folder.id] = FolderMatch(origin=folder, dest=new_dest_folder)

        for subfolder in folder.children:
            #Diretório de emails
            if (subfolder.folder_class == 'IPF.Note' or subfolder.name == 'Top of Information Store') and subfolder.name not in FOLDERS_IGNORE:
                self.traverse_and_create(subfolder, new_dest_folder)

    def add_contacts(self, acc_orig, acc_dest):
        folder_ori = acc_orig.contacts
        folder_dest = acc_dest.contacts
        self.map_folders[folder_ori.id] = FolderMatch(origin=folder_ori, dest=folder_dest)

    def add_calendars(self, acc_orig, acc_dest):
        folder_ori = acc_orig.calendar
        folder_dest = acc_dest.calendar
        self.map_folders[folder_ori.id] = FolderMatch(origin=folder_ori, dest=folder_dest)

class EmailMigrator:
    def __init__(self):
        pass

    def run(self, config, account_idx):

        self.tp = ThreadPool(config['general']['thread_count'])

        self.processed_total = 0

        credentials_orig = OAuth2Credentials(
            client_id=config['origin']['client_id'], client_secret=config['origin']['client_secret'], tenant_id=config['origin']['tenant_id']
        )
        config_orig = Configuration(
            retry_policy=FaultTolerance(max_wait=3600), credentials=credentials_orig, max_connections=config['origin']['connection_total']
        )

        credentials_dest = OAuth2Credentials(
            client_id=config['dest']['client_id'], client_secret=config['dest']['client_secret'], tenant_id=config['dest']['tenant_id']
        )
        config_dest = Configuration(
            retry_policy=FaultTolerance(max_wait=3600), credentials=credentials_dest, max_connections=config['dest']['connection_total']
        )

        origin_email = config['accounts'][account_idx]['origin']
        dest_email = config['accounts'][account_idx]['dest']

        self.logger = getLogger(dest_email)

        self.logger.info("Iniciando copia dos dados...")
        self.logger.info(f"Origem: {origin_email}")
        self.logger.info(f"Destino: {dest_email}")


        acc_orig = Account(origin_email, credentials=credentials_orig, autodiscover=True,  access_type=IMPERSONATION, config=config_orig)
        acc_dest = Account(dest_email, credentials=credentials_dest, autodiscover=True,  access_type=IMPERSONATION, config=config_dest)
        
        self.folder_migrator = ExchangeFolderMigrator()
        self.folder_migrator.add_contacts(acc_orig, acc_dest)
        self.folder_migrator.add_calendars(acc_orig, acc_dest)
        self.folder_migrator.traverse_and_create(acc_orig.root, acc_dest.root)

        initial_time = time.time()
        self.copy_items(acc_orig, acc_orig.root, acc_dest)
        total_time = time.time() - initial_time
        hours = int(total_time // 3600)
        minutes = int((total_time % 3600) // 60)
        seconds = int(total_time % 60)
        # Formatando o tempo no formato hh:mm:ss
        self.logger.info(f"Finalizado em {hours:10d}:{minutes:02d}:{seconds:02d}") 
    
        self.tp.close()



    def copy_items(self, acc_orig, folder_ori, acc_dest):

        for id, folder  in self.folder_migrator.map_folders.items():

            if folder.origin.absolute == '/root':
                continue

            while True:
                self.processed_total = 0
                q = Q(source_id__exists=False)
                er = folder.origin.filter(q)
                er.page_size = 200
                er.chunk_size = 5
                self.logger.info( f"Processando diretório {folder.origin.absolute}" )
                submitted_total = 0
                for item in er:
                    
                    submitted_total += 1
                    self.tp.add_task(self.process_item, acc_orig, acc_dest, item)

                    if submitted_total == 20:
                        self.tp.wait_completion()
                        submitted_total = 0

                self.tp.wait_completion()

                if self.processed_total == 0:
                    print( f"Diretório sem mais items {folder.origin.absolute}" )
                    break



    def process_item(self, acc_orig, acc_dest, item):
        copy = False
        
        if isinstance(item, Message) and item.item_class in ('IPM.Nota', 'IPM.Note'):
            parent = item.parent_folder_id.id
            dest_folder = self.folder_migrator.map_folders[parent].dest
            copy = True
        elif isinstance(item, Contact) or  isinstance(item, DistributionList) or  isinstance(item, Persona) :
            dest_folder = acc_dest.contacts
            copy = True
        elif isinstance(item, CalendarItem):
            dest_folder = acc_dest.calendar
            copy = True

        if copy:
            q = Q(source_id__exact=item.id)
            if dest_folder.filter(q).count() == 0:
                item.source_id = item.id
                item.save(update_fields=["source_id"])

                text = ''
                if hasattr(item, 'subject'):
                    text = item.subject
                if hasattr(item, 'display_name'):
                    text = item.display_name

                print( f"Copiando item {self.processed_total}: - {dest_folder.absolute} \ {text}" )
                self.processed_total = self.processed_total + 1
                data = acc_orig.export([item])
                acc_dest.upload((dest_folder, d) for d in data)
            else:
                print( f"Ignorando item copiado {item.id}" )
