from utils import FolderDiscovery, getLogger
import time
from exchangelib import FaultTolerance, Configuration
from exchangelib import IMPERSONATION, Account, CalendarItem, Message, OAuth2Credentials
from exchangelib import EWSDateTime, EWSTimeZone
from exchangelib.items import (
    Message,
    Contact, DistributionList, Persona
)
from exchangelib import Q
from thread import ThreadPool
from datetime import datetime, timedelta
import time

MAX_TASKS = 20

class EmailMigrator:
    def __init__(self):
        pass

    def run(self, config, account_idx):

        self.tp = ThreadPool(config['general']['thread_count'])

        self.copied_items = 0

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

        self.logger.info("Iniciando copia de emails...")
        self.logger.info(f"Conectando caixa de origem: {origin_email}")

        try:
            acc_orig = Account(origin_email, credentials=credentials_orig, autodiscover=True,  access_type=IMPERSONATION, config=config_orig)
        except Exception as e:
            self.logger.error(f"Erro ao conectar caixa de origem: {origin_email}, {e}")
            return
        
        self.logger.info(f"Conectando caixa de destino: {dest_email}")
        try:
            acc_dest = Account(dest_email, credentials=credentials_dest, autodiscover=True,  access_type=IMPERSONATION, config=config_dest)
        except Exception as e:
            self.logger.error(f"Erro ao conectar caixa de destino: {dest_email}, {e}")
            return

        self.logger.info(f"Descobrindo diretórios conhecidos...") 

        self.folder_migrator = FolderDiscovery(logger=self.logger, auto_create=True)

        self.folder_migrator.add_messages_folder(acc_orig, acc_dest)
        self.folder_migrator.add_contacts_folder(acc_orig, acc_dest)
        self.folder_migrator.add_calendars_folder(acc_orig, acc_dest)

        #Cria sub dos diretorios conhecidos
        self.folder_migrator.traverse_and_create(acc_orig.inbox, acc_dest.inbox)
        self.folder_migrator.traverse_and_create(acc_orig.outbox, acc_dest.outbox)
        self.folder_migrator.traverse_and_create(acc_orig.sent, acc_dest.sent)

        self.logger.info(f"Descobrindo diretórios customizados...") 
        self.folder_migrator.add_first_level(acc_orig, acc_dest)

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
                try:
                    self.copied_items = 0

                    tz = EWSTimeZone('America/Sao_Paulo')
                    date_init = EWSDateTime.from_datetime(datetime.now()).astimezone(tz) - timedelta(days=365)
                    q = Q(source_id__exists=False) & Q(datetime_received__gte=date_init)
                    er = folder.origin.filter(q).only('id', 'changekey', 'item_class', 'source_id')
                    er.page_size = 200
                    er.chunk_size = 5
                    total_items = er.count()
                    self.logger.info( f"Processando diretório {folder.origin.absolute} com {total_items} itens" )
                    submitted_total = 0
                    items_covered = 0
                    for item in er:
                        
                        submitted_total += 1
                        items_covered += 1

                        self.tp.add_task(self.process_item, acc_orig, acc_dest, item)
                        #print(item)
                        #self.process_item(acc_orig, acc_dest, item)

                        if submitted_total == MAX_TASKS:
                            self.tp.wait_completion()
                            self.logger.info( f"Processando diretório {folder.origin.absolute} - {items_covered}/{total_items} " )
                            submitted_total = 0

                    self.tp.wait_completion()

                    if self.copied_items == 0:
                        print( f"Diretório sem mais items {folder.origin.absolute}" )
                        break
                except Exception as e:
                    self.logger.warn("Erro, continuando...", e) 


    def process_item(self, acc_orig, acc_dest, item):
        copy = False
        
        if isinstance(item, Message) and item.item_class in ('IPM.Nota', 'IPM.Note'):
            items = acc_orig.fetch(ids=[(item.id, item.changekey)], only_fields=['parent_folder_id', 'subject', 'id', 'changekey', 'item_class', 'source_id'])
            item = next(items, None)

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
                try:
                    item.source_id = item.id
                    item.save(update_fields=["source_id"])

                    text = ''
                    if hasattr(item, 'subject'):
                        text = item.subject
                    if hasattr(item, 'display_name'):
                        text = item.display_name

                    print( f"Copiando item {item.id}: - {dest_folder.absolute}/{text}" )
                    self.copied_items += 1
                    data = acc_orig.export([item])
                    acc_dest.upload((dest_folder, d) for d in data)
                except Exception as e:
                    self.logger.warn("Erro ao copiar, continuando...", e)
            else:
                print( f"Ignorando item pois já foi copiado {item.id}" )
